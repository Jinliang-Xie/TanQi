import { AIMessage, BaseMessage } from '@langchain/core/messages';
import { ChatPromptTemplate } from '@langchain/core/prompts';
import { Annotation, StateGraph } from '@langchain/langgraph';
import { ChatOpenAI } from '@langchain/openai';
import { z } from 'zod';
import utils from './utils/tools/utils';
import * as path from 'path';
import * as fs from 'fs';

// 使用相对路径找到 Excel 文件，通过 __dirname 和 path.join 来构建
const file_path = path.join(__dirname, '..', 'T2C_dataset_Al.xlsx');

// 确保文件存在并可以被访问
if (!fs.existsSync(file_path)) {
  console.error(`错误: 文件 ${file_path} 不存在`);
}

// 获取工作表名称
let sheet_names: string;
try {
  sheet_names = utils.getSheetNames(file_path);
  console.log('成功读取工作表名称:', sheet_names);
} catch (error) {
  console.error('读取工作表名称时出错:', error);
  sheet_names = "无法读取工作表名称";
}

// 用于获取特定工作表数据的辅助函数
function getSheetData(sheetName: string, filterConditions?: {[key: string]: any}, extractColumns?: string[]): any[] {
  try {
    const jsonStr = utils.extractSheetToJson(
      file_path,
      sheetName,
      filterConditions,
      extractColumns
    );
    return JSON.parse(jsonStr);
  } catch (error) {
    console.error(`Error extracting data from sheet ${sheetName}:`, error);
    return [];
  }
}

// 根据流程UUID获取流数据
function getSelectedFlow(processUUID: string): any[] {
  return getSheetData(
    "flow_all",
    {'process_UUID': processUUID, 'Input/Output': 'Input'},
    ['flow_name', 'flow_UUID']
  );
}

const openai_api_key = process.env.OPENAI_API_KEY ?? '';
const openai_chat_model = process.env.OPENAI_CHAT_MODEL ?? '';

const stateAnnotation = Annotation.Root({
  messages: Annotation<BaseMessage[]>({
    reducer: (x, y) => x.concat(y),
  }),
  selected_flow: Annotation<any[]>(), // 存储已选择流的数据
  upstream_process_info: Annotation<any[]>(), // 存储上游处理过程信息
  upstream_flow_info: Annotation<any[]>(), // 存储上游流信息
  iteration_count: Annotation<number>({ // 添加迭代计数器
    default: () => 0, // 默认值为0
    reducer: (x, y) => x + y, // 累加计数器
  }),
  demands: Annotation<any[]>({ // 添加多需求数组
    default: () => [], // 默认为空数组
    reducer: (x, y) => y, // 完全替换，不累加
  }),
  current_demand_index: Annotation<number>({ // 添加当前处理的需求索引
    default: () => 0, // 默认值为0
    reducer: (x, y) => y, // 完全替换，不累加
  }),
  has_parallel_demands: Annotation<boolean>({ // 标记是否有多需求并行处理
    default: () => false, // 默认为false
    reducer: (x, y) => y, // 完全替换，不累加
  }),
});

async function demand_extractor(state: typeof stateAnnotation.State){
  const { messages, has_parallel_demands, demands, current_demand_index } = state;
  
  // 检查是否处理的是来自 workflow_restarter 的多需求
  // 
  if (has_parallel_demands && demands && demands.length > 0) {
    // 如果有多个需求，则获取当前需要处理的需求
    if (current_demand_index < demands.length) {
      const currentDemand = demands[current_demand_index];
      console.log(`处理第 ${current_demand_index + 1}/${demands.length} 个需求: ${currentDemand.flow_name}`);
      const query = currentDemand.content;
      
      const prompt = ChatPromptTemplate.fromTemplate(
        `You are an expert in the field of LCA, specializing in identifying specific unit processes within larger production chains. You will be given a corporate carbon footprint modeling requirement described in natural language. Your task is to extract the following key details from the text, with a focus on specificity and accuracy:
      - Process: Identify the most specific unit process involved in the final steps of production for the main product. Avoid general or broad descriptions; focus on the final, specific unit process where the main product is produced or finalized. Answer like "bauxite mining, main product: bauxite".
      - Technology: The specific process, method, or technology mentioned, related directly to the unit process.
      - Geographic Location: The country or region where the process takes place.
      - Time Frame: The year or period referred to in the requirement.
      Ensure that the extracted process details are as granular and specific as possible, corresponding to the smallest identifiable unit process directly involved in the production of the specified product.
      **The given query**: {query}`
      )
      
      const tool = {
        name: 'demand_extractor',
        description: 'Extracting demand from the given query.',
        schema: z.object({
          Process: z.string().describe('Extracted specific unit process'),
          Technology: z.string().describe('Extracted Technology'),
          geographicLocation: z.string().describe('Extracted Location'),
          timeFrame: z.string().describe('Extracted Time Frame'),
        }),
      }
    
      const model = new ChatOpenAI({
        apiKey: openai_api_key,
        modelName: openai_chat_model,
        temperature: 1,
        streaming: false,
      }).bindTools([tool], { tool_choice: tool.name });
    
      const chain = prompt.pipe(model);
      const response = await chain.invoke({ query: query }) as AIMessage;
    
      // 更新状态，将 current_demand_index 增加 1
      return {
        messages: [response],
        current_demand_index: current_demand_index + 1,
        // 如果已经处理完所有需求，则设置 has_parallel_demands 为 false
        has_parallel_demands: current_demand_index + 1 < demands.length
      }
    } else {
      // 所有需求都已处理完毕，重置状态
      console.log("所有并行需求已处理完毕");
      return {
        messages: [],
        has_parallel_demands: false,
        demands: [],
        current_demand_index: 0
      }
    }
  } else {
    // 普通处理逻辑（非多需求场景）
    const lastMessage = messages[messages.length - 1];
    const query = lastMessage.content;
    
    const prompt = ChatPromptTemplate.fromTemplate(
      `You are an expert in the field of LCA, specializing in identifying specific unit processes within larger production chains. You will be given a corporate carbon footprint modeling requirement described in natural language. Your task is to extract the following key details from the text, with a focus on specificity and accuracy:
    - Process: Identify the most specific unit process involved in the final steps of production for the main product. Avoid general or broad descriptions; focus on the final, specific unit process where the main product is produced or finalized. Answer like "bauxite mining, main product: bauxite".
    - Technology: The specific process, method, or technology mentioned, related directly to the unit process.
    - Geographic Location: The country or region where the process takes place.
    - Time Frame: The year or period referred to in the requirement.
    Ensure that the extracted process details are as granular and specific as possible, corresponding to the smallest identifiable unit process directly involved in the production of the specified product.
    **The given query**: {query}`
    )
    
    const tool = {
      name: 'demand_extractor',
      description: 'Extracting demand from the given query.',
      schema: z.object({
        Process: z.string().describe('Extracted specific unit process'),
        Technology: z.string().describe('Extracted Technology'),
        geographicLocation: z.string().describe('Extracted Location'),
        timeFrame: z.string().describe('Extracted Time Frame'),
      }),
    }
  
    const model = new ChatOpenAI({
      apiKey: openai_api_key,
      modelName: openai_chat_model,
      temperature: 1,
      streaming: false,
    }).bindTools([tool], { tool_choice: tool.name });
  
    const chain = prompt.pipe(model);
    const response = await chain.invoke({ query: query }) as AIMessage;
  
    return {
      messages: [response]
    }
  }
}

// 新增函数，用于处理是否需要继续处理下一个需求
function shouldProcessNextDemand(state: typeof stateAnnotation.State): string {
  const { has_parallel_demands, demands, current_demand_index } = state;
  
  // 检查是否有多需求并行处理
  if (has_parallel_demands && demands && demands.length > 0) {
    // 检查是否还有未处理的需求
    if (current_demand_index < demands.length) {
      console.log(`继续处理第 ${current_demand_index + 1}/${demands.length} 个需求`);
      return "process_next_demand";
    } else {
      console.log("所有并行需求已处理完毕，进入正常流程");
      return "normal_flow";
    }
  } else {
    console.log("没有并行需求，进入正常流程");
    return "normal_flow";
  }
}

async function title_matcher(state: typeof stateAnnotation.State){
  const { messages } = state;
  const lastMessage = messages[messages.length - 1] as AIMessage;
  const process_requirement = lastMessage.tool_calls?.[0]?.args?.Process ?? '';

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are acting as a title matcher for selecting relevant sheets from an Excel file based on the given product specification. Your task is to identify the two most appropriate sheet names that are semantically consistent with the provided product specification.
    
    Product Specification: "{process_requirement}"
    
    Sheet Names Available: {sheet_names}
    
    **Note: The sheet names are provided as a comma-separated string. You need to parse this string into individual sheet names before making your selection.**
    
    Instructions:
    1. First, parse the comma-separated sheet names into individual sheet names.
    2. Review the provided sheet names and identify the sheets that best match the product specification.
    3. Typically, the most suitable sheets start with "process" for process information and "flow" for flow information.
    4. Select one sheet name for the process and one for the flow that are most relevant and closely align with the product specification "{process_requirement}".
    5. Ensure your selections are based on the semantic consistency with the product specification.
    
    Use the title_matcher tool to provide your selections.`
  )
  
  const tool = {
    name: 'title_matcher',
    description: 'Match the title of process and flow',
    schema: z.object({
      selected_process_sheet: z.string().describe('The selected process sheet after matching'),
      selected_flow_sheet: z.string().describe('The selected flow sheet after matching'),
    }),
  }

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 1,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  const response = await chain.invoke({ 
    process_requirement: process_requirement,
    sheet_names: sheet_names  // This is already a comma-separated string from utils.getSheetNames
  }) as AIMessage;
  
  try {
    // Extract sheet names from the response
    const selected_process_sheet = response.tool_calls?.[0]?.args?.selected_process_sheet ?? '';
    const selected_flow_sheet = response.tool_calls?.[0]?.args?.selected_flow_sheet ?? '';

    console.log(`Selected sheets: Process=${selected_process_sheet}, Flow=${selected_flow_sheet}`);

    // Extract the actual data from the selected sheets
    const upstream_process_info_str = utils.extractSheetToJson(
      file_path,
      selected_process_sheet,
      {},
      ['process_UUID', 'process_name', 'location', 'flow_count']
    );
    
    const upstream_flow_info_str = utils.extractSheetToJson(
      file_path,
      selected_flow_sheet,
      {'reference': 'x'}, // 参考流
      ['process_UUID', 'flow_name', 'flow_category', 'flow_type']
    );
    
    console.log("Parsing upstream_process_info_str:", upstream_process_info_str);
    console.log("Parsing upstream_flow_info_str:", upstream_flow_info_str);

    // Parse the results, handling both successful responses and error responses
    let upstream_process_info = [];
    let upstream_flow_info = [];
    
    try {
      const processResult = JSON.parse(upstream_process_info_str);
      // Check if the response has an error field (our new format for error handling)
      if (processResult.error) {
        console.error("Error in process data:", processResult.error);
        upstream_process_info = processResult.data || [];
      } else {
        upstream_process_info = processResult;
      }
    } catch (error) {
      console.error("Failed to parse process info:", error);
      upstream_process_info = [];
    }

    try {
      const flowResult = JSON.parse(upstream_flow_info_str);
      // Check if the response has an error field
      if (flowResult.error) {
        console.error("Error in flow data:", flowResult.error);
        upstream_flow_info = flowResult.data || [];
      } else {
        upstream_flow_info = flowResult;
      }
    } catch (error) {
      console.error("Failed to parse flow info:", error);
      upstream_flow_info = [];
    }
    
    console.log("Processed upstream_flow_info:", upstream_flow_info);
    console.log("Processed upstream_process_info:", upstream_process_info);

    return {
      messages: [response],
      upstream_process_info: upstream_process_info,
      upstream_flow_info: upstream_flow_info
    }
  } catch (error) {
    console.error("Error in title_matcher function:", error);
    return {
      messages: [response],
      upstream_process_info: [],
      upstream_flow_info: []
    }
  }
}

async function technical_grader(state: typeof stateAnnotation.State){
  const { messages, upstream_process_info } = state;
  const lastMessage = messages[messages.length - 2] as AIMessage;
  const process_requirement = lastMessage.tool_calls?.[0]?.args?.Process ?? '';
  const technology_requirement = lastMessage.tool_calls?.[0]?.args?.Technology ?? '';

  // Use upstream_process_info from state if available, otherwise try to extract from message
  const process_info = upstream_process_info || 
    (messages[messages.length - 1] as any)?.upstream_process_info || 
    "No upstream process info found";

  const prompt = ChatPromptTemplate.fromTemplate(
    `You need to analyze each process in the upstream_process_info for technical representativeness.

    Grading criteria are as follows:
    - Grade 1:
      - The process's technology matches the technology_requirement.
      - The flow name matches or is close to the process_requirement.
    - Grade 2:
      - The process's technology is "generic" while the technology_requirement is specific, or vice versa.
      - The specific technology_requirement has only minor differences from other mainstream technology_requirements (i.e., low technical heterogeneity).
      - The flow name matches or is close to the process_requirement.
    - Grade 3:
      - The process's technology is "generic" while the technology_requirement is specific, or vice versa.
      - The specific technology_requirement has significant differences from other mainstream technology_requirements (i.e., high technical heterogeneity).
      - The flow name matches or is close to the process_requirement.
    - Grade 4:
      - The process's technology is specific.
      - The technology_requirement is a different specific type.
      - Both specific technology_requirements are similar in terms of system boundaries and carbon footprint.
      - The flow name matches or is close to the process_requirement.
    - Grade 5:
      - Any other situations.
    
    In the grading criteria, "specific" means the term is attributed to a specific name, brand, or technology, eg. "Bayer method" or "Soderberg method".
    Process's technology is included in the fourth position of process_name. For example, in the process_name "aluminium oxide production ; aluminium oxide, non-metallurgical ; bauxite ; generic ; 2015", the fourth position "generic" refers to the process_technology.
    Flow name is included in the second position of process_name. For example, in the process_name "aluminium oxide production ; aluminium oxide, non-metallurgical ; bauxite ; generic ; 2015", the second position "aluminium oxide, non-metallurgical" refers to the flow name.
    
    Use the technical_grader tool to record your assessment for each process.
    
    Process_requirements: {process_requirement}
    Technology_requirements: {technology_requirement}
    upstream_process_info: {upstream_process_info}`
  )
  
  const tool = {
    name: 'technical_grader',
    description: 'Grading the process in upstream_process_info on technical representativeness',
    schema: z.object({
      process_UUID: z.string().describe('UUID in upstream_process_info'),
      process_name: z.string().describe('Name of process in upstream_process_info'),
      location: z.string().describe('Location information of process in upstream_process_info'),
      flow_count: z.string().describe('The selected process after matching'),
      technical_representativeness: z.string().describe('The selected process after matching'),
      technical_type: z.string().describe('The selected process after matching'),
    }),
  }

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 1,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);

  // 调用chain三次，生成三个响应
  const response1 = await chain.invoke({ 
    process_requirement: process_requirement,
    technology_requirement: technology_requirement,
    upstream_process_info: process_info
  }) as AIMessage;
  
  const response2 = await chain.invoke({ 
    process_requirement: process_requirement,
    technology_requirement: technology_requirement,
    upstream_process_info: process_info
  }) as AIMessage;
  
  const response3 = await chain.invoke({ 
    process_requirement: process_requirement,
    technology_requirement: technology_requirement,
    upstream_process_info: process_info
  }) as AIMessage;
  
  // 将三个响应合并到messages数组中返回
  return {
    messages: [response1, response2, response3],
  }
}

async function spatial_grader(state: typeof stateAnnotation.State){
  const { messages, upstream_process_info } = state;
  const lastMessage = messages[messages.length - 2] as AIMessage;
  const geography_requirement = lastMessage.tool_calls?.[0]?.args?.geographicLocation ?? '';

  // Use upstream_process_info from state if available, otherwise try to extract from message
  const process_info = upstream_process_info || 
    (messages[messages.length - 1] as any)?.upstream_process_info || 
    "No upstream process info found";

  const prompt = ChatPromptTemplate.fromTemplate(
    `You need to analyze each process in the upstream_process_info for spatial representativeness.
    
    Grading criteria are as follows:
    - Grade 1:
      - The process's location is exactly the same as the geography_requirement.
    - Grade 2:
      - The process's location is a sub-region of the geography_requirement, or vice versa.
      - The larger region has low internal geographic heterogeneity.
    - Grade 3:
      - The process's location is a sub-region of the geography_requirement, or vice versa.
      - The larger region has high internal geographic heterogeneity.
    - Grade 4:
      - The process's location does not contain the geography_requirement, and the geography_requirement does not contain the process's location.
      - There is a strong similarity between the two.
    - Grade 5:
      - Any other situations.
    
    Use the spatial_grader tool to record your assessment for each process.
    
    Geography_requirements: {geography_requirement}
    upstream_process_info: {upstream_process_info}`
  )
  
  const tool = {
    name: 'spatial_grader',
    description: 'Grading the process in upstream_process_info on spatial representativeness',
    schema: z.object({
      process_UUID: z.string().describe('UUID in upstream_process_info'),
      process_name: z.string().describe('Name of process in upstream_process_info'),
      location: z.string().describe('Location information of process'),
      flow_count: z.string().describe('Flow count of the process'),
      spatial_representativeness: z.string().describe('Spatial representativeness grade'),
    }),
  }

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 1,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);

  const response1 = await chain.invoke({ 
    geography_requirement: geography_requirement,
    upstream_process_info: process_info
  }) as AIMessage;

  const response2 = await chain.invoke({ 
    geography_requirement: geography_requirement,
    upstream_process_info: process_info
  }) as AIMessage;

  const response3 = await chain.invoke({ 
    geography_requirement: geography_requirement,
    upstream_process_info: process_info
  }) as AIMessage;

  return {
    messages: [response1, response2, response3],
  }
}

async function time_grader(state: typeof stateAnnotation.State){
  const { messages, upstream_process_info } = state;
  const lastMessage = messages[messages.length - 2] as AIMessage;
  const time_requirement = lastMessage.tool_calls?.[0]?.args?.timeFrame ?? '';

  // Use upstream_process_info from state if available, otherwise try to extract from message
  const process_info = upstream_process_info || 
    (messages[messages.length - 1] as any)?.upstream_process_info || 
    "No upstream process info found";

  const prompt = ChatPromptTemplate.fromTemplate(
    `You need to analyze each process in the upstream_process_info for time representativeness.
    
    Grading criteria are as follows:
    - Grade 1: When the process's time frame is the same year as the time_requirement.
    - Grade 2: When the difference between the year of the process's time frame and the year of the time_requirement is more than 1 year but not more than 2 years.
    - Grade 3: When the difference between the year of the process's time frame and the year of the time_requirement is more than 2 years but not more than 3 years.
    - Grade 4: When the difference between the year of the process's time frame and the year of the time_requirement is more than 3 years but not more than 4 years.
    - Grade 5: When other situations occur, i.e., the difference between the year of the process's time frame and the year of the time_requirement is more than 4 years.
    
    Time frame is included in the last position of process_name. For example, in the process_name "aluminium oxide production ; aluminium oxide, non-metallurgical ; bauxite ; generic ; 2015", the last position "2015" refers to the time frame.
    
    Use the time_grader tool to record your assessment for each process.
    
    Time_requirements: {time_requirement}
    upstream_process_info: {upstream_process_info}`
  )
  
  const tool = {
    name: 'time_grader',
    description: 'Grading the process in upstream_process_info on time representativeness',
    schema: z.object({
      process_UUID: z.string().describe('UUID in upstream_process_info'),
      process_name: z.string().describe('Name of process in upstream_process_info'),
      flow_count: z.string().describe('Flow count of the process'),
      time_representativeness: z.string().describe('Time representativeness grade'),
    }),
  }

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 1,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  const response1 = await chain.invoke({ 
    time_requirement: time_requirement,
    upstream_process_info: process_info
  }) as AIMessage;

  const response2 = await chain.invoke({ 
    time_requirement: time_requirement,
    upstream_process_info: process_info
  }) as AIMessage;

  const response3 = await chain.invoke({ 
    time_requirement: time_requirement,
    upstream_process_info: process_info
  }) as AIMessage;

  return {
    messages: [response1, response2, response3],
  }
}

async function summarize_technical_grades(state: typeof stateAnnotation.State) {
  const { messages } = state;
  const technicalResults = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.[0]?.name === 'technical_grader'
  ) as AIMessage[];

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are tasked with summarizing the results from three technical graders.
    
    When summarizing the ratings:
    - If the technical representativeness ratings are consistent, use that as the final rating
    - If the ratings are inconsistent, use the most frequent result
    - If there are multiple most frequent results, use your judgment to select the most appropriate one
    
    Please analyze the following outputs and use the technical_summary tool to record your final assessment.

    Technical grader output 1: {output1}
    Technical grader output 2: {output2}
    Technical grader output 3: {output3}`
  );
  
  const tool = {
    name: 'technical_summary',
    description: 'Summarize technical grading results',
    schema: z.object({
      process_UUID: z.string().describe('UUID in upstream_process_info'),
      process_name: z.string().describe('Name of process in upstream_process_info'),
      flow_count: z.string().describe('Flow count of the process'),
      final_technical_representativeness: z.string().describe('Final technical grade'),
      other_results: z.array(z.string()).describe('Other grades that were considered')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const output1 = technicalResults[0]?.tool_calls?.[0]?.args || "No data";
  const output2 = technicalResults[1]?.tool_calls?.[0]?.args || "No data";
  const output3 = technicalResults[2]?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    output1: JSON.stringify(output1),
    output2: JSON.stringify(output2),
    output3: JSON.stringify(output3)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function summarize_spatial_grades(state: typeof stateAnnotation.State) {
  const { messages } = state;
  const spatialResults = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.[0]?.name === 'spatial_grader'
  ) as AIMessage[];

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are tasked with summarizing the results from three spatial graders.
    
    When summarizing the ratings:
    - If the spatial representativeness ratings are consistent, use that as the final rating
    - If the ratings are inconsistent, use the most frequent result
    - If there are multiple most frequent results, use your judgment to select the most appropriate one
    
    Please analyze the following outputs and use the spatial_summary tool to record your final assessment.

    Spatial grader output 1: {output1}
    Spatial grader output 2: {output2}
    Spatial grader output 3: {output3}`
  );
  
  const tool = {
    name: 'spatial_summary',
    description: 'Summarize spatial grading results',
    schema: z.object({
      process_UUID: z.string().describe('UUID in upstream_process_info'),
      process_name: z.string().describe('Name of process in upstream_process_info'),
      location: z.string().describe('Location information of process'),
      flow_count: z.string().describe('Flow count of the process'),
      final_spatial_representativeness: z.string().describe('Final spatial grade'),
      other_results: z.array(z.string()).describe('Other grades that were considered')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const output1 = spatialResults[0]?.tool_calls?.[0]?.args || "No data";
  const output2 = spatialResults[1]?.tool_calls?.[0]?.args || "No data";
  const output3 = spatialResults[2]?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    output1: JSON.stringify(output1),
    output2: JSON.stringify(output2),
    output3: JSON.stringify(output3)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function summarize_time_grades(state: typeof stateAnnotation.State) {
  const { messages } = state;
  const timeResults = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.[0]?.name === 'time_grader'
  ) as AIMessage[];

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are tasked with summarizing the results from three time graders.
    
    When summarizing the ratings:
    - If the time representativeness ratings are consistent, use that as the final rating
    - If the ratings are inconsistent, use the most frequent result
    - If there are multiple most frequent results, use your judgment to select the most appropriate one
    
    Please analyze the following outputs and use the time_summary tool to record your final assessment.

    Time grader output 1: {output1}
    Time grader output 2: {output2}
    Time grader output 3: {output3}`
  );
  
  const tool = {
    name: 'time_summary',
    description: 'Summarize time grading results',
    schema: z.object({
      process_UUID: z.string().describe('UUID in upstream_process_info'),
      process_name: z.string().describe('Name of process in upstream_process_info'),
      flow_count: z.string().describe('Flow count of the process'),
      final_time_representativeness: z.string().describe('Final time grade'),
      other_results: z.array(z.string()).describe('Other grades that were considered')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const output1 = timeResults[0]?.tool_calls?.[0]?.args || "No data";
  const output2 = timeResults[1]?.tool_calls?.[0]?.args || "No data";
  const output3 = timeResults[2]?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    output1: JSON.stringify(output1),
    output2: JSON.stringify(output2),
    output3: JSON.stringify(output3)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function final_summarizer(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find the summary messages
  const technicalSummary = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'technical_summary')
  ) as AIMessage;
  
  const spatialSummary = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'spatial_summary')
  ) as AIMessage;
  
  const timeSummary = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'time_summary')
  ) as AIMessage;

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are a professional in the field of LCA, specializing in summarizing representativeness of a unit process.

    You have been provided with individual summaries for technical, spatial, and time representativeness.
    
    Please analyze these summaries and create a comprehensive assessment that combines all three dimensions of representativeness.
    Use the final_summary tool to provide your assessment.

    Technical Summary: {technicalSummary}
    Spatial Summary: {spatialSummary}
    Time Summary: {timeSummary}`
  );
  
  const tool = {
    name: 'final_summary',
    description: 'Summarize overall representativeness results',
    schema: z.object({
      process_UUID: z.string().describe('UUID in upstream_process_info'),
      process_name: z.string().describe('Name of process in upstream_process_info'),
      location: z.string().describe('Location information of process'),
      flow_count: z.string().describe('Flow count of the process'),
      technical_representativeness: z.string().describe('Technical representativeness grade'),
      spatial_representativeness: z.string().describe('Spatial representativeness grade'),
      time_representativeness: z.string().describe('Time representativeness grade')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const technical = technicalSummary?.tool_calls?.[0]?.args || "No data";
  const spatial = spatialSummary?.tool_calls?.[0]?.args || "No data";
  const time = timeSummary?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    technicalSummary: JSON.stringify(technical),
    spatialSummary: JSON.stringify(spatial),
    timeSummary: JSON.stringify(time)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function heterogeneity_evaluator(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find the demand extractor output
  const demandExtractorOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'demand_extractor')
  ) as AIMessage;

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are a professional in the field of LCA. You have been asked to evaluate the spatial and temporal heterogeneity of a unit process.
    Process info: {process_info}
    
    You need to evaluate whether:
    - The spatial heterogeneity is very low
    - The temporal heterogeneity is very strong
    
    Use the heterogeneity_evaluator tool to provide your assessment.`
  );
  
  const tool = {
    name: 'heterogeneity_evaluator',
    description: 'Evaluate spatial and temporal heterogeneity of a unit process',
    schema: z.object({
      heterogeneity: z.enum(["RESULT_A", "RESULT_B"]).describe('RESULT_A if spatial heterogeneity is very low AND temporal heterogeneity is very strong; RESULT_B for any other combination')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const processInfo = demandExtractorOutput?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    process_info: JSON.stringify(processInfo)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function process_selector(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find the final summary and heterogeneity evaluator outputs
  const finalSummary = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'final_summary')
  ) as AIMessage;
  
  const heterogeneityEval = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'heterogeneity_evaluator')
  ) as AIMessage;

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are an expert in the field of Life Cycle Assessment (LCA), specializing in selecting the most suitable processes for a specific analysis. You have been provided with the following information:
    
    Process Information: {process_info}
    Downstream Process Heterogeneity: {heterogeneity}
    
    Based on the downstream process heterogeneity, please select the most suitable process considering technical, spatial, and temporal representativeness, as well as flow count. The selection criteria are as follows:
    
    If the evaluation result is RESULT_A:
      1. Technical Representativeness: Select all processes with the lowest technical representativeness.
      2. Temporal Representativeness: From the processes selected in step 1, select those with the lowest temporal representativeness.
      3. Spatial Representativeness: From the processes selected in step 2, select those with the lowest spatial representativeness.
      4. Flow Count: From the processes selected in step 3, select the process(es) with the highest flow count.
      5. Random Selection: If multiple processes remain after step 4, randomly select one process.
    
    If the evaluation result is RESULT_B:
      1. Technical Representativeness: Select all processes with the lowest technical representativeness.
      2. Spatial Representativeness: From the processes selected in step 1, select those with the lowest spatial representativeness.
      3. Temporal Representativeness: From the processes selected in step 2, select those with the lowest temporal representativeness.
      4. Flow Count: From the processes selected in step 3, select the process(es) with the highest flow count.
      5. Random Selection: If multiple processes remain after step 4, randomly select one process.
    
    Use the process_selector tool to provide your selection.`
  );
  
  const tool = {
    name: 'process_selector',
    description: 'Select the most suitable process for LCA',
    schema: z.object({
      process_UUID: z.string().describe('UUID of the selected process'),
      process_name: z.string().describe('Name of the selected process'),
      location: z.string().describe('Location of the selected process'),
      flow_count: z.string().describe('Flow count of the selected process')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const processInfo = finalSummary?.tool_calls?.[0]?.args || "No data";
  const heterogeneityInfo = heterogeneityEval?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    process_info: JSON.stringify(processInfo),
    heterogeneity: JSON.stringify(heterogeneityInfo)
  }) as AIMessage;
  
  // After selecting the process, fetch its associated flows
  const selectedProcessUUID = response.tool_calls?.[0]?.args?.process_UUID || "";
  
  // Get selected flows using the helper function
  const selectedFlows = getSelectedFlow(selectedProcessUUID);
  console.log(`Selected flows for process ${selectedProcessUUID}:`, selectedFlows);

  return {
    messages: [response],
    selected_flow: selectedFlows
  };
}

async function boundary_judger(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find the process selector output
  const selectorOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'process_selector')
  ) as AIMessage;

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are a professional in the field of LCA, specializing in boundary judgment. You have been provided with the following information:
    
    The following information is the process data of a life cycle unit.
    Process info: {process_info}
    
    Determine whether the process reaches the cradle of the life cycle. The cradle of the life cycle refers to the initial stage of the life cycle, where raw materials are extracted from nature.
    `
  );
  
  const tool = {
    name: 'boundary_judger',
    description: 'Judge whether the process reaches the cradle of the life cycle',
    schema: z.object({
      process_name: z.string().describe('Name of the process'),
      process_UUID: z.string().describe('UUID of the process'),
      whether_reach_cradle: z.enum(["Yes", "No"]).describe('Whether the process reaches the cradle')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const processInfo = selectorOutput?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    process_info: JSON.stringify(processInfo)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function industry_analyst(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find the process selector output
  const selectorOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'process_selector')
  ) as AIMessage;

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are a professional in the field of Life Cycle Assessment (LCA), specializing in industry analysis. You have been provided with information about a selected unit process:
    
    Process information: {selector_output}
    
    Based on the process name and characteristics, analyze and determine which industry this unit process belongs to.
    
    Use the industry_analyst tool to provide your analysis.`
  );
  
  const tool = {
    name: 'industry_analyst',
    description: 'Analyze which industry the process belongs to',
    schema: z.object({
      process_name: z.string().describe('Name of the process'),
      process_UUID: z.string().describe('UUID of the process'),
      process_industry: z.string().describe('Industry the process belongs to')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const selectorData = selectorOutput?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    selector_output: JSON.stringify(selectorData)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function flow_analyst(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find industry analyst output and selected flow data
  const selectorOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'process_selector')
  ) as AIMessage;
  
  const industryOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'industry_analyst')
  ) as AIMessage;
  
  const flowsMessage = messages.find(msg => msg.hasOwnProperty('selected_flow')) as any;
  const selectedFlow = flowsMessage?.selected_flow || [];

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are a professional in the field of LCA, specializing in flow analysis. You have been provided with:
    
    - Selected process information: {selector_output}
    - Industry of the selected process: {industry_output}
    - Input flows of the selected process: {selected_flow}
    
    Identify flows that are primarily used in the industry of the selected process and on which this industry is highly dependent. These are flows that:
    1. Are commonly used within the identified industry
    2. The identified industry has a high degree of dependence on these flows
    3. Would significantly impact the industry's operations if unavailable
    
    Use the flow_analyst tool to provide your analysis.`
  );
  
  const tool = {
    name: 'flow_analyst',
    description: 'Analyze industry-specific flows',
    schema: z.object({
      industry_specific_flows: z.array(z.object({
        flow_name: z.string().describe('Name of the flow'),
        flow_UUID: z.string().describe('UUID of the flow'),
        industry_relevance: z.enum(["high", "medium", "low"]).describe('Relevance to the industry')
      })).describe('List of flows specific to the industry')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const selectorData = selectorOutput?.tool_calls?.[0]?.args || "No data";
  const industryData = industryOutput?.tool_calls?.[0]?.args || "No data";
  
  const response = await chain.invoke({ 
    selector_output: JSON.stringify(selectorData),
    industry_output: JSON.stringify(industryData),
    selected_flow: JSON.stringify(selectedFlow)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function flow_judger(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find the three flow analyst outputs
  const flowAnalystResults = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'flow_analyst')
  ) as AIMessage[];

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are a professional in the field of LCA, specializing in flow evaluation. You have been provided with results from three parallel flow analysts:
    
    - Flow Analyst 1 output: {flow_analyst_output_1}
    - Flow Analyst 2 output: {flow_analyst_output_2}
    - Flow Analyst 3 output: {flow_analyst_output_3}
    
    Combine the results from all three analysts by:
    1. Taking the union of all identified industry-specific flows
    2. When the same flow appears in multiple analyses, keep the highest industry_relevance rating
    
    If the combined result contains no flows (empty set), set the empty_result flag to true.
    
    Use the flow_judger tool to provide your analysis.`
  );
  
  const tool = {
    name: 'flow_judger',
    description: 'Combine and evaluate flow analyst results',
    schema: z.object({
      combined_flows: z.array(z.object({
        flow_name: z.string().describe('Name of the flow'),
        flow_UUID: z.string().describe('UUID of the flow'),
        industry_relevance: z.enum(["high", "medium", "low"]).describe('Relevance to the industry')
      })).describe('Combined list of industry-specific flows'),
      empty_result: z.boolean().describe('Whether the combined result is empty')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  // Get the flow analyst outputs, handle the case where we might not have all three
  const output1 = flowAnalystResults[0]?.tool_calls?.[0]?.args || { industry_specific_flows: [] };
  const output2 = flowAnalystResults.length > 1 ? flowAnalystResults[1]?.tool_calls?.[0]?.args || { industry_specific_flows: [] } : { industry_specific_flows: [] };
  const output3 = flowAnalystResults.length > 2 ? flowAnalystResults[2]?.tool_calls?.[0]?.args || { industry_specific_flows: [] } : { industry_specific_flows: [] };
  
  const response = await chain.invoke({ 
    flow_analyst_output_1: JSON.stringify(output1),
    flow_analyst_output_2: JSON.stringify(output2),
    flow_analyst_output_3: JSON.stringify(output3)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function flow_filter(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find the flow judger output
  const flowJudgerOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'flow_judger')
  ) as AIMessage;

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are a professional in the field of LCA, specializing in flow filtering. You have been provided with:
    
    - Combined industry-specific flows: {flow_judger_output}
    - Input flows data which includes whether each flow is an "elementary flow" (basic flow directly from/to environment)
    
    For each flow, determine if it is an elementary flow based on the data provided.
    - If all flows are elementary flows, set all_elementary_flows to true.
    - If there are non-elementary flows and they are not more than three, include all of them.
    - If there are more than three non-elementary flows, select only the top three with the highest industry dependence.
    
    Use the flow_filter tool to provide your analysis.`
  );
  
  const tool = {
    name: 'flow_filter',
    description: 'Filter out elementary flows and select top non-elementary flows',
    schema: z.object({
      all_elementary_flows: z.boolean().describe('Whether all flows are elementary flows'),
      non_elementary_flows: z.array(z.object({
        flow_name: z.string().describe('Name of the flow'),
        flow_UUID: z.string().describe('UUID of the flow'),
        industry_relevance: z.enum(["high", "medium", "low"]).describe('Relevance to the industry')
      })).describe('List of non-elementary flows selected')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const flowJudgerData = flowJudgerOutput?.tool_calls?.[0]?.args || { combined_flows: [], empty_result: true };
  
  const response = await chain.invoke({ 
    flow_judger_output: JSON.stringify(flowJudgerData)
  }) as AIMessage;

  return {
    messages: [response],
  };
}

async function workflow_restarter(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find the flow filter output and demand extractor output
  const flowFilterOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'flow_filter')
  ) as AIMessage;
  
  const demandExtractorOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'demand_extractor')
  ) as AIMessage;

  const prompt = ChatPromptTemplate.fromTemplate(
    `You are a professional in the field of LCA, specializing in generating new upstream analysis demands. You have been provided with:
    
    - Original downstream demand: {downstream_demand}
    - Non-elementary flows for further analysis: {flow_filter_output}
    - Technical, temporal, and spatial information: {demand_extractor_output}
    
    Generate a new demand statement for each non-elementary flow, maintaining consistency with the original demand regarding:
    1. Time frame ({time_frame})
    2. Geographic location ({geographic_location})
    3. Industry context
    
    Each new demand should focus on the carbon footprint assessment of the specified non-elementary flow.
    
    Use the workflow_restarter tool to provide your new demand statements.`
  );
  
  const tool = {
    name: 'workflow_restarter',
    description: 'Generate new demand statements for non-elementary flows',
    schema: z.object({
      new_demands: z.array(z.object({
        flow_name: z.string().describe('Name of the flow'),
        flow_UUID: z.string().describe('UUID of the flow'),
        new_demand: z.string().describe('New demand statement')
      })).describe('New demands for further analysis')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0.2,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const filterData = flowFilterOutput?.tool_calls?.[0]?.args || { all_elementary_flows: true, non_elementary_flows: [] };
  const demandData = demandExtractorOutput?.tool_calls?.[0]?.args || {};
  const timeFrame = demandData.timeFrame || "current year";
  const geographicLocation = demandData.geographicLocation || "global";
  
  const response = await chain.invoke({ 
    downstream_demand: JSON.stringify(demandData),
    flow_filter_output: JSON.stringify(filterData),
    demand_extractor_output: JSON.stringify(demandData),
    time_frame: timeFrame,
    geographic_location: geographicLocation
  }) as AIMessage;

  // 从响应中提取生成的需求列表
  const newDemands = response.tool_calls?.[0]?.args?.new_demands || [];
  
  // 检查是否有需求被生成
  if (newDemands.length === 0) {
    console.log("没有生成新的需求，结束工作流程");
    return {
      messages: [response],
      has_parallel_demands: false,
      demands: [],
      current_demand_index: 0,
      iteration_count: 1 // 增加迭代计数
    };
  }

  console.log(`生成了 ${newDemands.length} 个新的需求`);
  
  // 为每个需求创建一个对象，将其保存到状态中
  const demandMessages = newDemands.map((demand: any) => {
    return { content: demand.new_demand, flow_name: demand.flow_name, flow_UUID: demand.flow_UUID }; 
  });
  
  // 返回更新后的状态
  return {
    messages: [response],
    has_parallel_demands: true,
    demands: demandMessages,
    current_demand_index: 0,
    iteration_count: 1 // 增加迭代计数
  };
}

// Decision functions for conditional routing
function shouldContinueBoundaryAnalysis(state: typeof stateAnnotation.State): string {
  const { messages } = state;
  
  // 检查历史迭代次数，防止无限循环
  // 计算已经进行的迭代次数，如果超过限制，则终止工作流
  const iterationCount = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'demand_extractor')
  ).length;
  
  if (iterationCount > 10) {
    console.log("已达到最大迭代次数限制 (10次)，终止工作流");
    return "end_workflow";
  }
  
  // Find the boundary judger output
  const judgerOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'boundary_judger')
  ) as AIMessage;
  
  if (judgerOutput) {
    try {
      const whether_reach_cradle = judgerOutput.tool_calls?.[0]?.args?.whether_reach_cradle;
      console.log("Boundary judger result:", whether_reach_cradle);
      
      // 严格检查返回值，确保它是有效的 Yes 或 No
      if (whether_reach_cradle === "Yes") {
        console.log("已达到生命周期的'摇篮'阶段，终止工作流");
        return "end_workflow";
      } else {
        console.log("尚未达到生命周期的'摇篮'阶段，继续分析");
        return "continue_analysis";
      }
    } catch (error) {
      console.error("处理boundary judger输出时出错:", error);
      // 在出现错误时默认终止，避免无限循环
      return "end_workflow";
    }
  }
  
  // 如果找不到有效结果，默认终止工作流
  console.log("无法找到boundary judger的有效输出，终止工作流");
  return "end_workflow";
}

function shouldContinueFlowAnalysis(state: typeof stateAnnotation.State): string {
  const { messages } = state;
  
  // 检查历史迭代次数，防止无限循环
  // 计算已经进行的迭代次数，如果超过限制，则终止工作流
  const iterationCount = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'flow_filter')
  ).length;
  
  if (iterationCount > 10) {
    console.log("已达到flow filter最大迭代次数限制 (10次)，终止工作流");
    return "end_workflow";
  }
  
  // Find the flow filter output
  const filterOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'flow_filter')
  ) as AIMessage;
  
  if (filterOutput) {
    try {
      const args = filterOutput.tool_calls?.[0]?.args;
      const allElementaryFlows = args?.all_elementary_flows || false;
      const nonElementaryFlows = args?.non_elementary_flows || [];
      
      console.log("Flow filter result:", { 
        allElementaryFlows, 
        nonElementaryFlowsCount: nonElementaryFlows.length 
      });
      
      // 如果所有流都是基本流，或者非基本流数量为0，则终止工作流
      if (allElementaryFlows === true || nonElementaryFlows.length === 0) {
        console.log("所有流都是基本流或没有非基本流，终止工作流");
        return "end_workflow";
      } else {
        console.log("存在非基本流需要进一步分析，继续工作流");
        return "continue_workflow";
      }
    } catch (error) {
      console.error("处理flow filter输出时出错:", error);
      // 在出现错误时默认终止，避免无限循环
      return "end_workflow";
    }
  }
  
  // 如果找不到有效的flow filter输出，默认终止工作流
  console.log("无法找到flow filter的有效输出，终止工作流");
  return "end_workflow";
}

function shouldContinueFlowJudger(state: typeof stateAnnotation.State): string {
  const { messages } = state;
  
  // 检查历史迭代次数，防止无限循环
  // 计算已经进行的迭代次数，如果超过限制，则终止工作流
  const iterationCount = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'flow_judger')
  ).length;
  
  if (iterationCount > 10) {
    console.log("已达到flow judger最大迭代次数限制 (10次)，终止工作流");
    return "end_workflow";
  }
  
  // Find the flow judger output
  const judgerOutput = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'flow_judger')
  ) as AIMessage;
  
  if (judgerOutput) {
    try {
      const args = judgerOutput.tool_calls?.[0]?.args;
      const emptyResult = args?.empty_result;
      const combinedFlows = args?.combined_flows || [];
      
      console.log("Flow judger result:", { emptyResult, flowsCount: combinedFlows.length });
      
      // 检查emptyResult标志或combinedFlows长度为0
      if (emptyResult === true || combinedFlows.length === 0) {
        console.log("合并后的流分析结果为空，终止工作流");
        return "end_workflow";
      } else {
        console.log("存在合并后的流分析结果，继续工作流");
        return "continue_workflow";
      }
    } catch (error) {
      console.error("处理flow judger输出时出错:", error);
      // 在出现错误时默认终止，避免无限循环
      return "end_workflow";
    }
  }
  
  // 如果找不到有效的flow judger输出，默认终止工作流
  console.log("无法找到flow judger的有效输出，终止工作流");
  return "end_workflow";
}

// Main workflow
const workflow = new StateGraph(stateAnnotation)
    .addNode("demand_extractor", demand_extractor)
    .addNode("title_matcher", title_matcher)
    .addNode("technical_grader", technical_grader)
    .addNode("spatial_grader", spatial_grader)
    .addNode("time_grader", time_grader)
    .addNode("technical_summary", summarize_technical_grades)
    .addNode("spatial_summary", summarize_spatial_grades)
    .addNode("time_summary", summarize_time_grades)
    .addNode("final_summary", final_summarizer)
    .addNode("heterogeneity_evaluator", heterogeneity_evaluator)
    .addNode("process_selector", process_selector)
    .addNode("boundary_judger", boundary_judger)
    .addNode("industry_analyst", industry_analyst)
    .addNode("flow_analyst", flow_analyst)
    .addNode("flow_judger", flow_judger)
    .addNode("flow_filter", flow_filter)
    .addNode("workflow_restarter", workflow_restarter)
    
    // Core workflow path
    .addEdge('__start__', "demand_extractor")
    
    // 添加 demand_extractor 的条件路由，处理多需求的并发
    .addConditionalEdges(
        "demand_extractor", 
        shouldProcessNextDemand,
        {
            // 如果还有更多需求要处理，循环回 demand_extractor
            "process_next_demand": "demand_extractor", 
            // 如果所有需求都处理完毕或者是单一需求，进入正常流程
            "normal_flow": "title_matcher"
        }
    )
    
    .addEdge("title_matcher", "technical_grader")
    .addEdge("title_matcher", "spatial_grader")
    .addEdge("title_matcher", "time_grader")
    .addEdge("technical_grader", "technical_summary")
    .addEdge("spatial_grader", "spatial_summary")
    .addEdge("time_grader", "time_summary")
    .addEdge("technical_summary", "final_summary")
    .addEdge("spatial_summary", "final_summary")
    .addEdge("time_summary", "final_summary")
    .addEdge("final_summary", "heterogeneity_evaluator")
    .addEdge("heterogeneity_evaluator", "process_selector")
    
    // Boundary delineating chain with conditional paths
    .addEdge("process_selector", "boundary_judger")
    // Decision point: If process reaches cradle, end workflow
    .addConditionalEdges(
        "boundary_judger", 
        shouldContinueBoundaryAnalysis, 
        {
            "end_workflow": "__end__",
            "continue_analysis": "industry_analyst"
        }
    )
    
    // Run three parallel flow analyst instances
    .addEdge("industry_analyst", "flow_analyst")
    .addEdge("industry_analyst", "flow_analyst") 
    .addEdge("industry_analyst", "flow_analyst")
    
    // Combine results from flow analysts
    .addEdge("flow_analyst", "flow_judger")
    
    // Decision point: If combined results are empty, end workflow
    .addConditionalEdges(
        "flow_judger",
        shouldContinueFlowJudger,
        {
            "end_workflow": "__end__",
            "continue_workflow": "flow_filter"
        }
    )
    
    // Decision point: If all flows are elementary, end workflow
    .addConditionalEdges(
        "flow_filter",
        shouldContinueFlowAnalysis,
        {
            "end_workflow": "__end__",
            "continue_workflow": "workflow_restarter"
        }
    )
    
    // Restart the workflow with new demands, 现在会生成多个并行需求
    .addEdge("workflow_restarter", "demand_extractor");

export const graph = workflow.compile();