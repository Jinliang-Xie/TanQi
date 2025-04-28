import { AIMessage, BaseMessage, HumanMessage } from '@langchain/core/messages';
import { StateGraph, Annotation } from '@langchain/langgraph';
import { graph as lcaKeyGraph } from './lcaKeyAgent';

// 定义需求类型
interface Demand {
  content: string;
  [key: string]: any;
}

// 定义状态注解
const stateAnnotation = Annotation.Root({
  messages: Annotation<BaseMessage[]>({
    reducer: (x, y) => x.concat(y),
  }),
  processSelectors: Annotation<AIMessage[]>({
    default: () => [],
    reducer: (x, y) => [...x, ...y],
  }),
  workflowRestarters: Annotation<AIMessage[]>({
    default: () => [],
    reducer: (x, y) => [...x, ...y],
  }),
  demands: Annotation<Demand[]>({
    default: () => [],
    reducer: (_, y) => y, // 替换而非追加
  }),
  errors: Annotation<string[]>({
    default: () => [],
    reducer: (x, y) => [...x, ...y],
  }),
});

// 用于跟踪已处理需求的集合，防止重复处理
const processedDemands = new Set<string>();

// 最大递归深度限制
const MAX_RECURSION_DEPTH = 3;

/**
 * 递归处理LCA需求并收集结果
 * @param demand 当前需求文本
 * @param processSelectors 收集processSelector结果的数组
 * @param workflowRestarters 收集workflowRestarter结果的数组
 * @param depth 当前递归深度（用于日志记录）
 * @returns 处理结果，包含可能的错误信息
 */
async function processDemandsRecursively(
  demand: string,
  processSelectors: AIMessage[],
  workflowRestarters: AIMessage[],
  depth: number = 0
): Promise<{error?: boolean, message?: string}> {
  const indent = ' '.repeat(depth * 2);
  console.log(`${indent}处理需求: ${demand}`);
  
  // 检查递归深度限制
  if (depth >= MAX_RECURSION_DEPTH) {
    const errorMessage = `${indent}达到最大递归深度 ${MAX_RECURSION_DEPTH}，停止递归处理`;
    console.error(errorMessage);
    return { error: true, message: errorMessage };
  }
  
  // 检查是否处理过相同/相似的需求
  const demandHash = demand.trim().toLowerCase().substring(0, 100);
  if (processedDemands.has(demandHash)) {
    console.log(`${indent}检测到重复/相似需求，跳过处理: ${demand.substring(0, 50)}...`);
    return {};
  }
  
  // 标记当前需求为已处理
  processedDemands.add(demandHash);
  
  try {
    // 执行LCA关键图与当前需求
    const result = await lcaKeyGraph.invoke({
      messages: [new HumanMessage(demand)]
    });
    
    // 查找process_selector结果
    const processSelectorMessage = result.messages.find(msg => 
      (msg as AIMessage).tool_calls?.some(tool => tool.name === 'process_selector')
    ) as AIMessage | undefined;
    
    if (processSelectorMessage) {
      console.log(`${indent}找到process_selector结果`);
      processSelectors.push(processSelectorMessage);
    }
    
    // 查找workflow_restarter结果
    const workflowRestarterMessage = result.messages.find(msg => 
      (msg as AIMessage).tool_calls?.some(tool => tool.name === 'workflow_restarter')
    ) as AIMessage | undefined;
    
    if (workflowRestarterMessage) {
      console.log(`${indent}找到workflow_restarter结果`);
      workflowRestarters.push(workflowRestarterMessage);
      
      // 检查是否有新的需求要处理
      const newDemands = result.demands || [];
      
      if (newDemands && newDemands.length > 0) {
        console.log(`${indent}发现 ${newDemands.length} 个新需求需要处理`);
        
        // 并发处理所有新需求，但限制并发数量
        const CONCURRENCY_LIMIT = 5;
        const processInBatches = async (demands: Demand[]) => {
          const results = [];
          for (let i = 0; i < demands.length; i += CONCURRENCY_LIMIT) {
            const batch = demands.slice(i, i + CONCURRENCY_LIMIT);
            const batchResults = await Promise.all(batch.map(demandObj => 
              processDemandsRecursively(
                demandObj.content, 
                processSelectors, 
                workflowRestarters,
                depth + 1
              )
            ));
            results.push(...batchResults);
          }
          return results;
        };

        await processInBatches(newDemands);
      } else {
        console.log(`${indent}没有新需求需要处理`);
      }
    }
    
    return {}; // 成功处理，无错误
  } catch (error) {
    const errorMessage = `${indent}处理需求时出错: ${error instanceof Error ? error.message : String(error)}`;
    console.error(errorMessage);
    return { error: true, message: errorMessage };
  }
}

/**
 * 通用工具结果格式化函数
 * @param state 当前状态
 * @param sourceKey 源数据在状态中的键名
 * @param toolName 要查找的工具名称
 * @param outputName 输出工具名称
 * @param outputLabel 输出标签（用于日志和消息内容）
 * @returns 更新后的状态
 */
async function formatToolResults(
  state: typeof stateAnnotation.State,
  sourceKey: string,
  toolName: string,
  outputName: string,
  outputLabel: string
) {
  const sourceData = state[sourceKey as keyof typeof state] as AIMessage[];
  
  const formattedResults = sourceData.map(msg => {
    const toolCall = msg.tool_calls?.find(tool => tool.name === toolName);
    return toolCall?.args || null;
  }).filter(Boolean);
  
  console.log(`格式化了 ${formattedResults.length} 个${outputLabel}结果`);
  
  const response = new AIMessage({
    content: `格式化了 ${formattedResults.length} 个${outputLabel}结果`,
    tool_calls: [{
      name: outputName,
      args: { results: formattedResults }
    }]
  });
  
  return {
    messages: [response]
  };
}

/**
 * 提取并格式化processSelector结果
 * @param state 当前状态
 * @returns 更新后的状态
 */
async function formatProcessSelectors(state: typeof stateAnnotation.State) {
  return formatToolResults(
    state,
    'processSelectors',
    'process_selector',
    'formatted_process_selectors',
    'process_selector'
  );
}

/**
 * 提取并格式化workflowRestarter结果
 * @param state 当前状态
 * @returns 更新后的状态
 */
async function formatWorkflowRestarters(state: typeof stateAnnotation.State) {
  return formatToolResults(
    state,
    'workflowRestarters',
    'workflow_restarter',
    'formatted_workflow_restarters',
    'workflow_restarter'
  );
}

/**
 * 分析完整生命周期
 * @param state 当前状态
 * @returns 更新后的状态
 */
async function analyzeCompleteLifeCycle(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // 获取初始需求
  const lastMessage = messages[messages.length - 1];
  const initialDemand = lastMessage.content as string;
  
  console.log("开始完整生命周期分析...");
  
  // 清空已处理需求的集合，确保每次工作流开始时重置
  processedDemands.clear();
  
  // 用于收集结果的数组
  const processSelectors: AIMessage[] = [];
  const workflowRestarters: AIMessage[] = [];
  const errors: string[] = [];
  
  // 处理初始需求及其所有子需求
  const result = await processDemandsRecursively(initialDemand, processSelectors, workflowRestarters);
  if (result.error && result.message) {
    errors.push(result.message);
  }
  
  console.log(`分析完成。收集了 ${processSelectors.length} 个process_selector结果和 ${workflowRestarters.length} 个workflow_restarter结果`);
  if (errors.length > 0) {
    console.log(`处理过程中出现了 ${errors.length} 个错误`);
  }
  
  const response = new AIMessage({
    content: "生命周期分析完成",
    tool_calls: [{
      name: 'lifecycle_analysis_complete',
      args: { 
        processSelectorCount: processSelectors.length,
        workflowRestarterCount: workflowRestarters.length,
        errorCount: errors.length
      }
    }]
  });
  
  return {
    messages: [response],
    processSelectors,
    workflowRestarters,
    errors
  };
}

/**
 * 总结分析结果
 * @param state 当前状态
 * @returns 更新后的状态
 */
async function summarizeResults(state: typeof stateAnnotation.State) {
  const { messages, errors } = state;
  
  // 从先前的格式化节点获取结果
  const formattedSelectorsMsg = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'formatted_process_selectors')
  ) as AIMessage | undefined;
  
  const formattedRestartersMsg = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'formatted_workflow_restarters')
  ) as AIMessage | undefined;
  
  // 提取格式化后的结果
  const processSelectors = formattedSelectorsMsg?.tool_calls?.find(
    t => t.name === 'formatted_process_selectors'
  )?.args?.results || [];
  
  const workflowRestarters = formattedRestartersMsg?.tool_calls?.find(
    t => t.name === 'formatted_workflow_restarters'
  )?.args?.results || [];
  
  console.log(`总结分析: ${processSelectors.length} 个process_selector结果和 ${workflowRestarters.length} 个workflow_restarter结果`);
  
  const response = new AIMessage({
    content: "分析结果总结完成",
    tool_calls: [{
      name: 'analysis_summary',
      args: { 
        processSelectors,
        workflowRestarters,
        errors: errors || [],
        errorCount: (errors || []).length
      }
    }]
  });
  
  return {
    messages: [response]
  };
}

// 构建工作流图
const workflow = new StateGraph(stateAnnotation)
  .addNode("analyzeCompleteLifeCycle", analyzeCompleteLifeCycle)
  .addNode("formatProcessSelectors", formatProcessSelectors)
  .addNode("formatWorkflowRestarters", formatWorkflowRestarters)
  .addNode("summarizeResults", summarizeResults)
  .addEdge("__start__", "analyzeCompleteLifeCycle")
  .addEdge("analyzeCompleteLifeCycle", "formatProcessSelectors")
  .addEdge("formatProcessSelectors", "formatWorkflowRestarters")
  .addEdge("formatWorkflowRestarters", "summarizeResults")
  .addEdge("summarizeResults", "__end__");

// 编译并导出图
export const graph = workflow.compile();