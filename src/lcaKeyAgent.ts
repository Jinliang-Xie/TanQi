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
function getSelectedFlow(processUUID: string, state?: typeof stateAnnotation.State): any[] {
  // 获取动态选择的flow sheet名称
  const sheetName = state?.selected_flow_sheet || "";
  
  // 如果没有选定的工作表，记录错误并返回空数组
  if (!sheetName) {
    console.error(`没有可用的flow sheet，无法获取流程 ${processUUID} 的流数据`);
    return [];
  }
  
  console.log(`使用工作表 "${sheetName}" 获取流程 ${processUUID} 的流数据`);
  
  return getSheetData(
    sheetName,
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
  selected_flow_sheet: Annotation<string>({ // 存储title_matcher选择的flow工作表名称
    default: () => "", // 默认为空字符串，不再使用flow_all作为默认值
    reducer: (_, y) => y, // 直接替换当前值，忽略旧值
  }),
  iteration_count: Annotation<number>({ // 添加迭代计数器
    default: () => 0, // 默认值为0
    reducer: (x, y) => x + y, // 累加计数器
  }),
  demands: Annotation<any[]>({ // 添加多需求数组
    default: () => [], // 默认为空数组
    reducer: (_, y) => y, // 完全替换，不累加，忽略旧值
  }),
});

async function demand_extractor(state: typeof stateAnnotation.State){
  const { messages } = state;
  
  // Get the last message content as the query
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
      upstream_flow_info: upstream_flow_info,
      selected_flow_sheet: selected_flow_sheet // 保存选择的flow工作表名称到状态中
    }
  } catch (error) {
    console.error("Error in title_matcher function:", error);
    return {
      messages: [response],
      upstream_process_info: [],
      upstream_flow_info: [],
      selected_flow_sheet: "" // 使用空字符串，不再使用默认值flow_all
    }
  }
}

async function technical_grader(state: typeof stateAnnotation.State){
  const { messages, upstream_process_info } = state;
  const lastMessage = messages[messages.length - 2] as AIMessage;
  const process_requirement = lastMessage.tool_calls?.[0]?.args?.Process ?? '';
  const technology_requirement = lastMessage.tool_calls?.[0]?.args?.Technology ?? '';
  
  let processInfo: any[] = [];

  // Check if we have upstream_process_info from title_matcher
  if (!upstream_process_info || upstream_process_info.length === 0) {
    console.log("No upstream_process_info provided from title_matcher, falling back to all processes");
    
    // Fallback mechanism: collect process data from all process sheets
    const allSheetNames = sheet_names.split(',').filter(name => name.trim().startsWith('process_'));
    processInfo = [];
    
    // Collect process data from all process sheets
    for (const sheetName of allSheetNames) {
      try {
        const sheetData = getSheetData(
          sheetName.trim(),
          {},
          ['process_UUID', 'process_name', 'location', 'flow_count']
        );
        processInfo = [...processInfo, ...sheetData];
      } catch (error) {
        console.error(`Error extracting data from sheet ${sheetName}:`, error);
      }
    }

    console.log(`Collected ${processInfo.length} total processes from all sheets for grading`);
  } else {
    // Use the filtered upstream_process_info from title_matcher
    processInfo = upstream_process_info;
    console.log(`Using ${processInfo.length} processes filtered by title_matcher for technical grading`);
  }
  
  const prompt = ChatPromptTemplate.fromTemplate(
    `You need to analyze the process for technical representativeness.

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
    
    Use the technical_grader tool to record your assessment for this process.
    
    Process_requirements: {process_requirement}
    Technology_requirements: {technology_requirement}
    process_info: {process_info}`
  )
  
  const tool = {
    name: 'technical_grader',
    description: 'Grading the process on technical representativeness',
    schema: z.object({
      process_UUID: z.string().describe('UUID of the process'),
      process_name: z.string().describe('Name of the process'),
      location: z.string().describe('Location information of the process'),
      flow_count: z.string().describe('Flow count of the process'),
      technical_representativeness: z.string().describe('Technical representativeness grade (1-5)'),
      technical_type: z.string().describe('Type of technology used'),
    }),
  }

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);

  console.log(`Processing ${processInfo.length} processes concurrently for technical grading`);
  
  // Process concurrently with batching
  const batchSize = 5; // Process 5 items in parallel at a time
  const allResponses: AIMessage[] = [];
  const allGradedProcesses: any[] = [];
  
  // Process in batches
  for (let i = 0; i < processInfo.length; i += batchSize) {
    const currentBatch = processInfo.slice(i, i + batchSize);
    console.log(`Processing technical grading batch ${i/batchSize + 1} of ${Math.ceil(processInfo.length/batchSize)}`);
    
    const batchPromises = currentBatch.map(async (process, index) => {
      try {
        console.log(`Started grading process ${i + index + 1}/${processInfo.length}: ${process.process_name}`);
        
        const response = await chain.invoke({ 
          process_requirement: process_requirement,
          technology_requirement: technology_requirement,
          process_info: JSON.stringify(process)
        }) as AIMessage;
        
        console.log(`Completed grading process ${i + index + 1}/${processInfo.length}`);
        
        // Extract the graded process from the response
        if (response.tool_calls && response.tool_calls.length > 0) {
          const toolCall = response.tool_calls[0];
          if (toolCall.name === 'technical_grader' && toolCall.args) {
            return { response, gradedProcess: toolCall.args };
          }
        }
        
        return { response, gradedProcess: null };
      } catch (error) {
        console.error(`Error grading process ${process.process_UUID}:`, error);
        return { response: null, gradedProcess: null, error };
      }
    });
    
    // Wait for all promises in the current batch to resolve
    const batchResults = await Promise.all(batchPromises);
    
    // Collect responses and graded processes
    for (const result of batchResults) {
      if (result.response) allResponses.push(result.response);
      if (result.gradedProcess) allGradedProcesses.push(result.gradedProcess);
    }
  }
  
  console.log(`Total graded processes: ${allGradedProcesses.length} out of ${processInfo.length}`);
  
  // Verify all processes were graded
  const gradedProcessUUIDs = new Set(allGradedProcesses.map(process => process.process_UUID));
  const allProcessUUIDs = new Set(processInfo.map(process => process.process_UUID));
  
  if (gradedProcessUUIDs.size !== allProcessUUIDs.size) {
    console.warn(`Warning: Not all processes were graded. Expected ${allProcessUUIDs.size}, got ${gradedProcessUUIDs.size}`);
    
    // Log which processes weren't graded
    const missingUUIDs = [...allProcessUUIDs].filter(uuid => !gradedProcessUUIDs.has(uuid));
    console.warn(`Missing grades for processes: ${missingUUIDs.join(', ')}`);
  }
  
  return {
    messages: allResponses,
    all_process_grades: allGradedProcesses,
  }
}

async function spatial_grader(state: typeof stateAnnotation.State){
  const { messages, upstream_process_info } = state;
  const lastMessage = messages[messages.length - 2] as AIMessage;
  const geography_requirement = lastMessage.tool_calls?.[0]?.args?.geographicLocation ?? '';

  let processInfo: any[];

  // Check if we have upstream_process_info from title_matcher
  if (!upstream_process_info || upstream_process_info.length === 0) {
    console.log("No upstream_process_info provided from title_matcher, falling back to all processes");
    
    // Fallback mechanism: collect process data from all process sheets
    const allSheetNames = sheet_names.split(',').filter(name => name.trim().startsWith('process_'));
    processInfo = [];
    
    // Collect process data from all process sheets
    for (const sheetName of allSheetNames) {
      try {
        const sheetData = getSheetData(
          sheetName.trim(),
          {},
          ['process_UUID', 'process_name', 'location', 'flow_count']
        );
        processInfo = [...processInfo, ...sheetData];
      } catch (error) {
        console.error(`Error extracting data from sheet ${sheetName}:`, error);
      }
    }

    console.log(`Collected ${processInfo.length} total processes from all sheets for spatial grading`);
  } else {
    // Use the filtered upstream_process_info from title_matcher
    processInfo = upstream_process_info;
    console.log(`Using ${processInfo.length} processes filtered by title_matcher for spatial grading`);
  }

  const prompt = ChatPromptTemplate.fromTemplate(
    `You need to analyze the process for spatial representativeness.
    
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
    
    Use the spatial_grader tool to record your assessment for this process.
    
    Geography_requirements: {geography_requirement}
    process_info: {process_info}`
  )
  
  const tool = {
    name: 'spatial_grader',
    description: 'Grading the process on spatial representativeness',
    schema: z.object({
      process_UUID: z.string().describe('UUID of the process'),
      process_name: z.string().describe('Name of the process'),
      location: z.string().describe('Location information of the process'),
      flow_count: z.string().describe('Flow count of the process'),
      spatial_representativeness: z.string().describe('Spatial representativeness grade (1-5)'),
    }),
  }

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);

  console.log(`Processing ${processInfo.length} processes concurrently for spatial grading`);
  
  // Process concurrently with batching
  const batchSize = 5; // Process 5 items in parallel at a time
  const allResponses: AIMessage[] = [];
  const allGradedProcesses: any[] = [];
  
  // Process in batches
  for (let i = 0; i < processInfo.length; i += batchSize) {
    const currentBatch = processInfo.slice(i, i + batchSize);
    console.log(`Processing spatial grading batch ${i/batchSize + 1} of ${Math.ceil(processInfo.length/batchSize)}`);
    
    const batchPromises = currentBatch.map(async (process, index) => {
      try {
        console.log(`Started spatial grading process ${i + index + 1}/${processInfo.length}: ${process.process_name}`);
        
        const response = await chain.invoke({ 
          geography_requirement: geography_requirement,
          process_info: JSON.stringify(process)
        }) as AIMessage;
        
        console.log(`Completed spatial grading process ${i + index + 1}/${processInfo.length}`);
        
        // Extract the graded process from the response
        if (response.tool_calls && response.tool_calls.length > 0) {
          const toolCall = response.tool_calls[0];
          if (toolCall.name === 'spatial_grader' && toolCall.args) {
            return { response, gradedProcess: toolCall.args };
          }
        }
        
        return { response, gradedProcess: null };
      } catch (error) {
        console.error(`Error in spatial grading process ${process.process_UUID}:`, error);
        return { response: null, gradedProcess: null, error };
      }
    });
    
    // Wait for all promises in the current batch to resolve
    const batchResults = await Promise.all(batchPromises);
    
    // Collect responses and graded processes
    for (const result of batchResults) {
      if (result.response) allResponses.push(result.response);
      if (result.gradedProcess) allGradedProcesses.push(result.gradedProcess);
    }
  }
  
  console.log(`Total spatially graded processes: ${allGradedProcesses.length} out of ${processInfo.length}`);
  
  // Verify all processes were graded
  const gradedProcessUUIDs = new Set(allGradedProcesses.map(process => process.process_UUID));
  const allProcessUUIDs = new Set(processInfo.map(process => process.process_UUID));
  
  if (gradedProcessUUIDs.size !== allProcessUUIDs.size) {
    console.warn(`Warning: Not all processes were spatially graded. Expected ${allProcessUUIDs.size}, got ${gradedProcessUUIDs.size}`);
    
    // Log which processes weren't graded
    const missingUUIDs = [...allProcessUUIDs].filter(uuid => !gradedProcessUUIDs.has(uuid));
    console.warn(`Missing spatial grades for processes: ${missingUUIDs.join(', ')}`);
  }

  return {
    messages: allResponses,
    all_spatial_grades: allGradedProcesses,
  }
}

async function time_grader(state: typeof stateAnnotation.State){
  const { messages, upstream_process_info } = state;
  const lastMessage = messages[messages.length - 2] as AIMessage;
  const time_requirement = lastMessage.tool_calls?.[0]?.args?.timeFrame ?? '';
  
  let processInfo: any[];

  // Check if we have upstream_process_info from title_matcher
  if (!upstream_process_info || upstream_process_info.length === 0) {
    console.log("No upstream_process_info provided from title_matcher, falling back to all processes");
    
    // Fallback mechanism: collect process data from all process sheets
    const allSheetNames = sheet_names.split(',').filter(name => name.trim().startsWith('process_'));
    processInfo = [];
    
    // Collect process data from all process sheets
    for (const sheetName of allSheetNames) {
      try {
        const sheetData = getSheetData(
          sheetName.trim(),
          {},
          ['process_UUID', 'process_name', 'location', 'flow_count']
        );
        processInfo = [...processInfo, ...sheetData];
      } catch (error) {
        console.error(`Error extracting data from sheet ${sheetName}:`, error);
      }
    }

    console.log(`Collected ${processInfo.length} total processes from all sheets for time grading`);
  } else {
    // Use the filtered upstream_process_info from title_matcher
    processInfo = upstream_process_info;
    console.log(`Using ${processInfo.length} processes filtered by title_matcher for time grading`);
  }

  const prompt = ChatPromptTemplate.fromTemplate(
    `You need to analyze the process for time representativeness.
    
    Grading criteria are as follows:
    - Grade 1: When the process's time frame is the same year as the time_requirement.
    - Grade 2: When the difference between the year of the process's time frame and the year of the time_requirement is more than 1 year but not more than 2 years.
    - Grade 3: When the difference between the year of the process's time frame and the year of the time_requirement is more than 2 years but not more than 3 years.
    - Grade 4: When the difference between the year of the process's time frame and the year of the time_requirement is more than 3 years but not more than 4 years.
    - Grade 5: When other situations occur, i.e., the difference between the year of the process's time frame and the year of the time_requirement is more than 4 years.
    
    Time frame is included in the last position of process_name. For example, in the process_name "aluminium oxide production ; aluminium oxide, non-metallurgical ; bauxite ; generic ; 2015", the last position "2015" refers to the time frame.
    
    Use the time_grader tool to record your assessment for this process.
    
    Time_requirements: {time_requirement}
    process_info: {process_info}`
  )
  
  const tool = {
    name: 'time_grader',
    description: 'Grading the process on time representativeness',
    schema: z.object({
      process_UUID: z.string().describe('UUID of the process'),
      process_name: z.string().describe('Name of the process'),
      flow_count: z.string().describe('Flow count of the process'),
      time_representativeness: z.string().describe('Time representativeness grade (1-5)'),
    }),
  }

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);

  console.log(`Processing ${processInfo.length} processes concurrently for time grading`);
  
  // Process concurrently with batching
  const batchSize = 5; // Process 5 items in parallel at a time
  const allResponses: AIMessage[] = [];
  const allGradedProcesses: any[] = [];
  
  // Process in batches
  for (let i = 0; i < processInfo.length; i += batchSize) {
    const currentBatch = processInfo.slice(i, i + batchSize);
    console.log(`Processing time grading batch ${i/batchSize + 1} of ${Math.ceil(processInfo.length/batchSize)}`);
    
    const batchPromises = currentBatch.map(async (process, index) => {
      try {
        console.log(`Started time grading process ${i + index + 1}/${processInfo.length}: ${process.process_name}`);
        
        const response = await chain.invoke({ 
          time_requirement: time_requirement,
          process_info: JSON.stringify(process)
        }) as AIMessage;
        
        console.log(`Completed time grading process ${i + index + 1}/${processInfo.length}`);
        
        // Extract the graded process from the response
        if (response.tool_calls && response.tool_calls.length > 0) {
          const toolCall = response.tool_calls[0];
          if (toolCall.name === 'time_grader' && toolCall.args) {
            return { response, gradedProcess: toolCall.args };
          }
        }
        
        return { response, gradedProcess: null };
      } catch (error) {
        console.error(`Error in time grading process ${process.process_UUID}:`, error);
        return { response: null, gradedProcess: null, error };
      }
    });
    
    // Wait for all promises in the current batch to resolve
    const batchResults = await Promise.all(batchPromises);
    
    // Collect responses and graded processes
    for (const result of batchResults) {
      if (result.response) allResponses.push(result.response);
      if (result.gradedProcess) allGradedProcesses.push(result.gradedProcess);
    }
  }
  
  console.log(`Total time graded processes: ${allGradedProcesses.length} out of ${processInfo.length}`);
  
  // Verify all processes were graded
  const gradedProcessUUIDs = new Set(allGradedProcesses.map(process => process.process_UUID));
  const allProcessUUIDs = new Set(processInfo.map(process => process.process_UUID));
  
  if (gradedProcessUUIDs.size !== allProcessUUIDs.size) {
    console.warn(`Warning: Not all processes were time graded. Expected ${allProcessUUIDs.size}, got ${gradedProcessUUIDs.size}`);
    
    // Log which processes weren't graded
    const missingUUIDs = [...allProcessUUIDs].filter(uuid => !gradedProcessUUIDs.has(uuid));
    console.warn(`Missing time grades for processes: ${missingUUIDs.join(', ')}`);
  }

  return {
    messages: allResponses,
    all_time_grades: allGradedProcesses,
  }
}

async function summarize_technical_grades(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find all technical grades from the messages
  const allTechnicalGrades = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.[0]?.name === 'technical_grader'
  ) as AIMessage[];

  // Group grades by process UUID to handle batch results
  const processGrades: {[key: string]: any[]} = {};
  
  for (const gradeMsg of allTechnicalGrades) {
    const gradeData = gradeMsg.tool_calls?.[0]?.args;
    if (gradeData) {
      const processUUID = gradeData.process_UUID;
      if (!processGrades[processUUID]) {
        processGrades[processUUID] = [];
      }
      processGrades[processUUID].push(gradeData);
    }
  }

  // For each process, summarize its grades
  const allSummaries: AIMessage[] = [];
  const processUUIDs = Object.keys(processGrades);
  
  console.log(`Summarizing technical grades for ${processUUIDs.length} processes`);
  
  const prompt = ChatPromptTemplate.fromTemplate(
    `You are tasked with summarizing the technical representativeness grades for a process.
    
    When summarizing the ratings:
    - If the technical representativeness ratings are consistent, use that as the final rating
    - If the ratings are inconsistent, use the most frequent result
    - If there are multiple most frequent results, use your judgment to select the most appropriate one
    
    Process UUID: {process_uuid}
    Process Name: {process_name}
    Technical grades to summarize: {grades}
    
    Use the technical_summary tool to record your final assessment.`
  );
  
  const tool = {
    name: 'technical_summary',
    description: 'Summarize technical grading results',
    schema: z.object({
      process_UUID: z.string().describe('UUID of the process'),
      process_name: z.string().describe('Name of the process'),
      flow_count: z.string().describe('Flow count of the process'),
      final_technical_representativeness: z.string().describe('Final technical grade (1-5)'),
      other_results: z.array(z.string()).describe('Other grades that were considered')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  // Process each UUID in batches if needed
  for (const processUUID of processUUIDs) {
    const grades = processGrades[processUUID];
    if (grades.length === 0) continue;
    
    try {
      const processName = grades[0].process_name || "Unknown Process";
      
      const response = await chain.invoke({ 
        process_uuid: processUUID,
        process_name: processName,
        grades: JSON.stringify(grades)
      }) as AIMessage;
      
      allSummaries.push(response);
    } catch (error) {
      console.error(`Error summarizing technical grades for process ${processUUID}:`, error);
    }
  }

  return {
    messages: allSummaries,
    all_technical_summaries: allSummaries,
  };
}

async function summarize_spatial_grades(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find all spatial grades from the messages
  const allSpatialGrades = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.[0]?.name === 'spatial_grader'
  ) as AIMessage[];

  // Group grades by process UUID
  const processGrades: {[key: string]: any[]} = {};
  
  for (const gradeMsg of allSpatialGrades) {
    const gradeData = gradeMsg.tool_calls?.[0]?.args;
    if (gradeData) {
      const processUUID = gradeData.process_UUID;
      if (!processGrades[processUUID]) {
        processGrades[processUUID] = [];
      }
      processGrades[processUUID].push(gradeData);
    }
  }

  // For each process, summarize its grades
  const allSummaries: AIMessage[] = [];
  const processUUIDs = Object.keys(processGrades);
  
  console.log(`Summarizing spatial grades for ${processUUIDs.length} processes`);
  
  const prompt = ChatPromptTemplate.fromTemplate(
    `You are tasked with summarizing the spatial representativeness grades for a process.
    
    When summarizing the ratings:
    - If the spatial representativeness ratings are consistent, use that as the final rating
    - If the ratings are inconsistent, use the most frequent result
    - If there are multiple most frequent results, use your judgment to select the most appropriate one
    
    Process UUID: {process_uuid}
    Process Name: {process_name}
    Spatial grades to summarize: {grades}
    
    Use the spatial_summary tool to record your final assessment.`
  );
  
  const tool = {
    name: 'spatial_summary',
    description: 'Summarize spatial grading results',
    schema: z.object({
      process_UUID: z.string().describe('UUID of the process'),
      process_name: z.string().describe('Name of the process'),
      location: z.string().describe('Location information of the process'),
      flow_count: z.string().describe('Flow count of the process'),
      final_spatial_representativeness: z.string().describe('Final spatial grade (1-5)'),
      other_results: z.array(z.string()).describe('Other grades that were considered')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  for (const processUUID of processUUIDs) {
    const grades = processGrades[processUUID];
    if (grades.length === 0) continue;
    
    try {
      const processName = grades[0].process_name || "Unknown Process";
      const location = grades[0].location || "Unknown Location";
      
      const response = await chain.invoke({ 
        process_uuid: processUUID,
        process_name: processName,
        grades: JSON.stringify(grades)
      }) as AIMessage;
      
      allSummaries.push(response);
    } catch (error) {
      console.error(`Error summarizing spatial grades for process ${processUUID}:`, error);
    }
  }

  return {
    messages: allSummaries,
    all_spatial_summaries: allSummaries,
  };
}

async function summarize_time_grades(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  // Find all time grades from the messages
  const allTimeGrades = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.[0]?.name === 'time_grader'
  ) as AIMessage[];

  // Group grades by process UUID
  const processGrades: {[key: string]: any[]} = {};
  
  for (const gradeMsg of allTimeGrades) {
    const gradeData = gradeMsg.tool_calls?.[0]?.args;
    if (gradeData) {
      const processUUID = gradeData.process_UUID;
      if (!processGrades[processUUID]) {
        processGrades[processUUID] = [];
      }
      processGrades[processUUID].push(gradeData);
    }
  }

  // For each process, summarize its grades
  const allSummaries: AIMessage[] = [];
  const processUUIDs = Object.keys(processGrades);
  
  console.log(`Summarizing time grades for ${processUUIDs.length} processes`);
  
  const prompt = ChatPromptTemplate.fromTemplate(
    `You are tasked with summarizing the time representativeness grades for a process.
    
    When summarizing the ratings:
    - If the time representativeness ratings are consistent, use that as the final rating
    - If the ratings are inconsistent, use the most frequent result
    - If there are multiple most frequent results, use your judgment to select the most appropriate one
    
    Process UUID: {process_uuid}
    Process Name: {process_name}
    Time grades to summarize: {grades}
    
    Use the time_summary tool to record your final assessment.`
  );
  
  const tool = {
    name: 'time_summary',
    description: 'Summarize time grading results',
    schema: z.object({
      process_UUID: z.string().describe('UUID of the process'),
      process_name: z.string().describe('Name of the process'),
      flow_count: z.string().describe('Flow count of the process'),
      final_time_representativeness: z.string().describe('Final time grade (1-5)'),
      other_results: z.array(z.string()).describe('Other grades that were considered')
    }),
  };

  const model = new ChatOpenAI({
    apiKey: openai_api_key,
    modelName: openai_chat_model,
    temperature: 0,
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  for (const processUUID of processUUIDs) {
    const grades = processGrades[processUUID];
    if (grades.length === 0) continue;
    
    try {
      const processName = grades[0].process_name || "Unknown Process";
      
      const response = await chain.invoke({ 
        process_uuid: processUUID,
        process_name: processName,
        grades: JSON.stringify(grades)
      }) as AIMessage;
      
      allSummaries.push(response);
    } catch (error) {
      console.error(`Error summarizing time grades for process ${processUUID}:`, error);
    }
  }

  return {
    messages: allSummaries,
    all_time_grades: allSummaries, // Store all grades for later use
  }
}

async function final_summarizer(state: typeof stateAnnotation.State) {
  const { messages } = state;
  
  console.log("Starting final summarizer - collecting grade data");
  
  // Find the summary messages by type
  const technicalSummaries = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'technical_summary')
  ) as AIMessage[];
  
  const spatialSummaries = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'spatial_summary')
  ) as AIMessage[];
  
  const timeSummaries = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'time_summary')
  ) as AIMessage[];

  // Create a mapping of all processes and their grades
  const allProcesses: {[key: string]: any} = {};
  
  // Add technical grades
  for (const summary of technicalSummaries) {
    const data = summary.tool_calls?.[0]?.args;
    if (data && data.process_UUID) {
      if (!allProcesses[data.process_UUID]) {
        allProcesses[data.process_UUID] = {
          process_UUID: data.process_UUID,
          process_name: data.process_name,
          flow_count: data.flow_count,
        };
      }
      
      allProcesses[data.process_UUID].technical_grade = data.final_technical_representativeness;
    }
  }
  
  // Add spatial grades
  for (const summary of spatialSummaries) {
    const data = summary.tool_calls?.[0]?.args;
    if (data && data.process_UUID) {
      if (!allProcesses[data.process_UUID]) {
        allProcesses[data.process_UUID] = {
          process_UUID: data.process_UUID,
          process_name: data.process_name,
          location: data.location,
          flow_count: data.flow_count,
        };
      } else if (data.location) {
        allProcesses[data.process_UUID].location = data.location;
      }
      
      allProcesses[data.process_UUID].spatial_grade = data.final_spatial_representativeness;
    }
  }
  
  // Add time grades
  for (const summary of timeSummaries) {
    const data = summary.tool_calls?.[0]?.args;
    if (data && data.process_UUID) {
      if (!allProcesses[data.process_UUID]) {
        allProcesses[data.process_UUID] = {
          process_UUID: data.process_UUID,
          process_name: data.process_name,
          flow_count: data.flow_count,
        };
      }
      
      allProcesses[data.process_UUID].time_grade = data.final_time_representativeness;
    }
  }

  // Create final summaries for each process - directly without LLM
  const finalSummaries: AIMessage[] = [];
  const processUUIDs = Object.keys(allProcesses);
  
  console.log(`Creating final summaries for ${processUUIDs.length} processes (optimized method)`);
  
  // Directly create synthetic AIMessages with the required tool_calls structure
  for (const processUUID of processUUIDs) {
    const processData = allProcesses[processUUID];
    if (!processData.technical_grade || !processData.spatial_grade || !processData.time_grade) {
      console.log(`Skipping process ${processUUID} due to missing grades`);
      continue;
    }
    
    try {
      // Create a synthetic AIMessage with the expected tool_calls structure
      const summary = {
        process_UUID: processUUID,
        process_name: processData.process_name || "Unknown Process",
        location: processData.location || "Unknown Location",
        flow_count: processData.flow_count || "0",
        technical_representativeness: processData.technical_grade,
        spatial_representativeness: processData.spatial_grade,
        time_representativeness: processData.time_grade
      };
      
      // Create a synthetic AIMessage mimicking the structure from LLM responses
      const aiMessage = new AIMessage({
        content: "",
        tool_calls: [{
          name: 'final_summary',
          args: summary
        }]
      });
      
      finalSummaries.push(aiMessage);
      
    } catch (error) {
      console.error(`Error creating final summary for process ${processUUID}:`, error);
    }
  }

  console.log(`Finished creating ${finalSummaries.length} final summaries without LLM calls`);

  return {
    messages: finalSummaries,
    all_process_summaries: finalSummaries, // Store all summaries for later steps
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
  
  // Find ALL final summaries instead of just one
  const finalSummaries = messages.filter(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'final_summary')
  ) as AIMessage[];
  
  const heterogeneityEval = messages.find(msg => 
    (msg as AIMessage).tool_calls?.some(tool => tool.name === 'heterogeneity_evaluator')
  ) as AIMessage;

  // Extract process data from all summaries
  const allProcesses = finalSummaries.map(summary => {
    const data = summary.tool_calls?.[0]?.args;
    if (data) return data;
    return null;
  }).filter(Boolean);

  console.log(`Collected ${allProcesses.length} processes for selection evaluation`);
  
  const prompt = ChatPromptTemplate.fromTemplate(
    `You are an expert in the field of Life Cycle Assessment (LCA), specializing in selecting the most suitable processes. The process data provided contains representativeness grades where LOWER NUMBERS INDICATE BETTER REPRESENTATIVENESS (1 is the best, 5 is the worst).
    
    Process Information: {process_info}
    Downstream Process Heterogeneity: {heterogeneity}
    
    Your task is to select the most suitable process following these steps in strict order:
    
    If the evaluation result is RESULT_A:
      1. FIRST filter for processes with the NUMERICALLY LOWEST technical_representativeness value (1 is best, 5 is worst).
      2. THEN from those remaining, select processes with the NUMERICALLY LOWEST temporal representativeness value.
      3. THEN from those remaining, select processes with the NUMERICALLY LOWEST spatial representativeness value.
      4. THEN from those remaining, select the process with the HIGHEST flow_count value.
      5. If multiple processes remain equal after all steps, select one from them using your judgment.
    
    If the evaluation result is RESULT_B:
      1. FIRST filter for processes with the NUMERICALLY LOWEST technical_representativeness value (1 is best, 5 is worst).
      2. THEN from those remaining, select processes with the NUMERICALLY LOWEST spatial representativeness value.
      3. THEN from those remaining, select processes with the NUMERICALLY LOWEST temporal representativeness value.
      4. THEN from those remaining, select the process with the HIGHEST flow_count value.
      5. If multiple processes remain equal after all steps, select one from them using your judgment.
    
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
    modelName: "gpt-4.1",
    streaming: false,
  }).bindTools([tool], { tool_choice: tool.name });

  const chain = prompt.pipe(model);
  
  const heterogeneityInfo = heterogeneityEval?.tool_calls?.[0]?.args || "No data";
  
  // Pass all processes instead of just one
  const response = await chain.invoke({ 
    process_info: JSON.stringify(allProcesses),
    heterogeneity: JSON.stringify(heterogeneityInfo)
  }) as AIMessage;
  
  // After selecting the process, fetch its associated flows
  const selectedProcessUUID = response.tool_calls?.[0]?.args?.process_UUID || "";
  
  console.log(`Selected process UUID: ${selectedProcessUUID}`);
  
  // Get selected flows using the helper function with state参数
  const selectedFlows = getSelectedFlow(selectedProcessUUID, state);
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
      demands: [],
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
    demands: demandMessages,
  };
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
    .addEdge('__start__', "demand_extractor")
    .addEdge("demand_extractor", "title_matcher")
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
    .addEdge("process_selector", "boundary_judger")
    .addEdge("process_selector", "industry_analyst")
    .addEdge("industry_analyst", "flow_analyst")
    .addEdge("flow_analyst", "flow_judger")
    .addEdge("flow_judger", "flow_filter")
    .addEdge("flow_filter", "workflow_restarter")
    .addEdge("boundary_judger", "__end__")
    .addEdge("workflow_restarter", "__end__");

export const graph = workflow.compile();