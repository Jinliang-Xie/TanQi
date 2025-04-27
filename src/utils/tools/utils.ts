import * as fs from 'fs';
import * as path from 'path';
import * as xlsx from 'xlsx';

interface FilterConditions {
    [key: string]: any;
}

interface ProcessData {
    process_UUID: string;
    [key: string]: any;
}

/**
 * 从输入的字符串中提取并解析JSON内容。如果提取或解析失败，则返回原始字符串。
 * @param jsonStr 包含JSON内容的字符串
 * @returns 解析后的JSON数据或原始字符串
 */
function cleanJson(jsonStr: string): any {
    try {
        // 使用正则表达式查找包含在 ```json 和 ``` 之间的JSON内容
        const pattern = /```json\s*(\{.*?\})\s*```/s;
        const match = pattern.exec(jsonStr);
        
        // 如果未找到匹配的JSON内容，则返回原始字符串
        if (!match) {
            return jsonStr;
        }
        
        // 提取匹配到的JSON字符串并去除多余的空白字符
        const jsonCleaned = match[1].trim();
        
        // 尝试解析提取到的JSON字符串
        return JSON.parse(jsonCleaned);
    } catch (e) {
        // 捕获异常并返回原始字符串
        console.error(`Error: ${e}`);
        return jsonStr;
    }
}

/**
 * 获取Excel文件中所有sheet的名称
 * @param filePath Excel文件路径
 * @returns 所有sheet名称的逗号分隔字符串
 */
function getSheetNames(filePath: string): string {
    try {
      console.log('尝试读取Excel文件路径:', filePath);
      
      if (!fs.existsSync(filePath)) {
        console.error(`文件不存在: ${filePath}`);
        return "文件不存在";
      }
      
      const workbook = xlsx.readFile(filePath);
      return workbook.SheetNames.join(', ');
    } catch (error) {
      console.error(`读取Excel文件错误: ${error}`);
      return `读取文件错误`;
    }
}

/**
 * 从Excel表格中提取特定sheet的特定列，并根据筛选条件进行筛选，最后返回JSON字符串。
 * @param filePath Excel文件路径
 * @param sheetName 需要读取的sheet名称
 * @param filterConditions 筛选条件，例如：{'列名1': '筛选值1', '列名2': '筛选值2'}
 * @param extractColumns 需要提取的列名列表
 * @returns JSON字符串格式的筛选并提取后的数据
 */
function extractSheetToJson(
    filePath: string, 
    sheetName: string, 
    filterConditions?: FilterConditions, 
    extractColumns?: string[]
): string {
    try {
        // 使用文件路径，不再使用path.resolve做额外的处理，因为路径应该已经在lcaKeyAgent.ts中处理好了
        console.log('尝试读取Excel文件路径:', filePath);
        
        // 检查文件是否存在
        if (!fs.existsSync(filePath)) {
            console.error(`文件不存在: ${filePath}`);
            return JSON.stringify({ error: "文件不存在，请检查文件路径。", data: [] });
        }
        
        // 读取Excel文件
        const workbook = xlsx.readFile(filePath);
        
        // 检查sheet名称是否存在
        if (!workbook.SheetNames.includes(sheetName)) {
            console.error(`Sheet名称不存在: ${sheetName}`);
            return JSON.stringify({ error: `Sheet名称 '${sheetName}' 不存在，请检查Sheet名称。`, data: [] });
        }
        
        // 获取指定sheet的数据
        const worksheet = workbook.Sheets[sheetName];
        
        // 将工作表转换为JSON
        let data: any[] = xlsx.utils.sheet_to_json(worksheet);
        
        // 处理筛选条件
        if (filterConditions) {
            try {
                for (const [column, value] of Object.entries(filterConditions)) {
                    if (!data[0] || !(column in data[0])) {
                        console.error(`筛选条件中的列不存在: ${column}`);
                        return JSON.stringify({ error: `筛选条件中的列 '${column}' 不存在。`, data: [] });
                    }
                    data = data.filter(row => row[column] === value);
                }
            } catch (e) {
                console.error(`筛选时出现问题: ${e}`);
                return JSON.stringify({ error: `筛选时出现问题。详情: ${e}`, data: [] });
            }
        }
        
        // 提取指定的列，如果extractColumns为undefined，则提取所有列
        if (extractColumns) {
            try {
                // 检查数组是否为空
                if (data.length === 0) {
                    console.error(`工作表为空，无法提取列`);
                    return JSON.stringify({ error: `工作表为空，无法提取列`, data: [] });
                }
                
                // 检查是否有完全缺失的列（所有记录中都不存在）
                const completelyMissingColumns = extractColumns.filter(col => 
                    data.every(row => !(col in row))
                );
                
                if (completelyMissingColumns.length > 0) {
                    console.error(`以下列在所有记录中都不存在: ${completelyMissingColumns.join(', ')}`);
                    console.log(`可用的列: ${Object.keys(data[0]).join(', ')}`);
                    // 列出所有记录中的唯一列名
                    const allColumns = new Set<string>();
                    data.forEach(row => {
                        Object.keys(row).forEach(col => allColumns.add(col));
                    });
                    console.log(`所有记录中的列: ${Array.from(allColumns).join(', ')}`);
                }
                
                // 即使有列缺失，也继续处理，只是用默认值填充
                data = data.map(row => {
                    const newRow: { [key: string]: any } = {};
                    extractColumns.forEach(col => {
                        // 如果列不存在，设置为null或其他默认值
                        newRow[col] = col in row ? row[col] : null;
                    });
                    return newRow;
                });
            } catch (e) {
                console.error(`提取列时出现问题: ${e}`);
                return JSON.stringify({ error: `提取列时出现问题。详情: ${e}`, data: [] });
            }
        }
        
        // 转换为JSON字符串
        return JSON.stringify(data);
        
    } catch (e) {
        console.error(`读取Excel文件错误: ${e}`);
        if (e instanceof Error && e.message.includes('ENOENT')) {
            return JSON.stringify({ error: "文件未找到，请检查文件路径。", data: [] });
        }
        return JSON.stringify({ error: `无法读取Excel文件。详情: ${e}`, data: [] });
    }
}

/**
 * 加载Excel数据
 * @param filePath Excel文件路径
 * @param sheetName sheet名称
 * @returns 加载的数据数组
 */
function loadExcel(filePath: string, sheetName: string): any[] {
    try {
        console.log('加载Excel文件:', filePath, '工作表:', sheetName);
        
        // 检查文件是否存在
        if (!fs.existsSync(filePath)) {
            console.error(`文件不存在: ${filePath}`);
            return [];
        }
        
        const workbook = xlsx.readFile(filePath);
        
        // 检查sheet名称是否存在
        if (!workbook.SheetNames.includes(sheetName)) {
            console.error(`Sheet名称 '${sheetName}' 不存在`);
            return [];
        }
        
        const worksheet = workbook.Sheets[sheetName];
        return xlsx.utils.sheet_to_json(worksheet);
    } catch (error) {
        console.error(`加载Excel数据错误: ${error}`);
        return [];
    }
}

/**
 * 提取下游处理过程数据
 * @param filePath Excel文件路径
 * @param processUUID 流程UUID
 * @returns 过滤后的过程数据JSON字符串
 */
function extractDownstreamProcess(filePath: string, processUUID: string): string {
    try {
        // 先加载数据，这样会有错误处理
        const data = loadExcel(filePath, 'Process');
        if (data.length === 0) {
            return JSON.stringify({ error: "无法加载Process工作表", data: [] });
        }
        
        const filteredData = data.filter(row => row.process_UUID === processUUID);
        const mappedData = filteredData.map(row => ({
            process_UUID: row.process_UUID,
            process_name: row.process_name,
            location: row.location,
            validity_start: row.validity_start
        }));
        
        return JSON.stringify(mappedData);
    } catch (error) {
        console.error(`提取下游处理过程数据错误: ${error}`);
        return JSON.stringify({ error: `提取下游处理过程数据错误: ${error}`, data: [] });
    }
}

/**
 * 提取下游流程数据
 * @param filePath Excel文件路径
 * @param processUUID 流程UUID
 * @returns 过滤后的流程数据JSON字符串
 */
function extractDownstreamFlow(filePath: string, processUUID: string): string {
    try {
        // 先加载数据，这样会有错误处理
        const data = loadExcel(filePath, 'Flow');
        if (data.length === 0) {
            return JSON.stringify({ error: "无法加载Flow工作表", data: [] });
        }
        
        const filteredData = data.filter(row => 
            row.process_UUID === processUUID && 
            row['Input/Output'] === 'Input'
        );
        
        const mappedData = filteredData.map(row => ({
            flow_name: row.flow_name,
            flow_UUID: row.flow_UUID,
            flow_classification: row.flow_classification,
            flow_type: row.flow_type,
            process_UUID: row.process_UUID
        }));
        
        return JSON.stringify(mappedData);
    } catch (error) {
        console.error(`提取下游流程数据错误: ${error}`);
        return JSON.stringify({ error: `提取下游流程数据错误: ${error}`, data: [] });
    }
}

/**
 * 获取流程ID数组
 * @param processData 流程数据数组
 * @returns 流程UUID数组
 */
function getProcessIds(processData: ProcessData[]): string[] {
    return processData.map(item => item.process_UUID);
}

/**
 * 提取上游流程数据
 * @param jsonStr JSON字符串
 * @param filePath Excel文件路径
 * @returns 提取的上游流程数据JSON字符串
 */
function extractUpstreamFlow(jsonStr: string, filePath: string): string {
    // 清理JSON字符串并提取数据
    const processData = cleanJson(jsonStr) as ProcessData[];
    const processIds = getProcessIds(processData);
    
    // 读取Excel文件
    const df = loadExcel(filePath, 'Flow');
    
    // 筛选数据
    const filteredDf = df.filter(row => 
        row.reference === 'Siu' &&
        row['Input/Output'] === 'Output' &&
        processIds.includes(row.process_UUID)
    );
    
    // 选择指定的列
    const selectedColumns = filteredDf.map(row => ({
        process_UUID: row.process_UUID,
        process_name: row.process_name,
        flow_name: row.flow_name,
        flow_UUID: row.flow_UUID,
        flow_classification: row.flow_classification,
        flow_type: row.flow_type
    }));
    
    // 转换为JSON格式
    return JSON.stringify(selectedColumns);
}

/**
 * 提取上游处理过程数据
 * @param jsonStr JSON字符串
 * @param filePath Excel文件路径
 * @returns 上游处理过程数据JSON字符串
 */
function extractUpstreamProcess(jsonStr: string, filePath: string): string {
    // 清理JSON字符串并提取数据
    const processData = cleanJson(jsonStr) as ProcessData[];
    const processIds = getProcessIds(processData);
    
    // 读取Excel文件
    const df = loadExcel(filePath, 'Process');
    
    // 筛选数据
    const filteredDf = df.filter(row => 
        processIds.includes(row.process_UUID)
    );
    
    // 选择指定的列
    const selectedColumns = filteredDf.map(row => ({
        process_UUID: row.process_UUID,
        process_name: row.process_name,
        technical_type: row.technical_type,
        location: row.location,
        validity_start: row.validity_start,
        flow_count: row.flow_count
    }));
    
    // 转换为JSON格式
    return JSON.stringify(selectedColumns);
}

/**
 * 提取上游处理过程技术数据
 * @param jsonStr JSON字符串
 * @param filePath Excel文件路径
 * @returns 上游处理过程技术数据JSON字符串
 */
function extractUpstreamProcessTechnique(jsonStr: string, filePath: string): string {
    // 清理JSON字符串并提取数据
    const processData = cleanJson(jsonStr) as ProcessData[];
    const processIds = getProcessIds(processData);
    
    // 读取Excel文件
    const flowDf = loadExcel(filePath, 'Flow');
    const processDf = loadExcel(filePath, 'Process');
    
    // 筛选Flow页的数据
    const filteredFlowDf = flowDf.filter(row => 
        row.reference === 'Siu' &&
        row['Input/Output'] === 'Output' &&
        processIds.includes(row.process_UUID)
    );
    
    // 筛选Process页的technical_type列
    const technicalTypeDf = processDf.map(row => ({
        process_UUID: row.process_UUID,
        technical_type: row.technical_type
    }));
    
    // 合并两个数据集
    const mergedDf = filteredFlowDf.map(flowRow => {
        const processRow = technicalTypeDf.find(
            row => row.process_UUID === flowRow.process_UUID
        );
        
        return {
            process_UUID: flowRow.process_UUID,
            process_name: flowRow.process_name,
            flow_name: flowRow.flow_name,
            flow_classification: flowRow.flow_classification,
            flow_type: flowRow.flow_type,
            technical_type: processRow ? processRow.technical_type : null
        };
    });
    
    // 转换为JSON格式
    return JSON.stringify(mergedDf);
}

/**
 * 提取下游需求数据
 * @param filePath Excel文件路径
 * @param processUUID 流程UUID
 * @param techniqueType 技术类型
 * @returns 格式化的JSON字符串
 */
function extractDownstreamDemand(filePath: string, processUUID: string, techniqueType: string): string {
    // 从 Process 页中提取数据
    const processData = loadExcel(filePath, 'Process');
    const filteredProcessData = processData.filter(row => row.process_UUID === processUUID);
    const processInfo = filteredProcessData.map(row => ({
        location: row.location,
        validity_start: row.validity_start
    }));
    
    // 从 Flow 页中提取数据
    const flowData = loadExcel(filePath, 'Flow');
    const filteredFlowData = flowData.filter(row => 
        row.process_UUID === processUUID && 
        row.reference === 'Siu'
    );
    
    const flowInfo = filteredFlowData.map(row => ({
        flow_name: row.flow_name,
        flow_classification: row.flow_classification,
        flow_type: row.flow_type
    }));
    
    // 合并数据
    const combinedData = flowInfo.map((flowItem, index) => {
        const processItem = processInfo[0] || {};
        
        return {
            flow_name: flowItem.flow_name,
            technique_type: techniqueType,
            flow_classification: flowItem.flow_classification,
            flow_type: flowItem.flow_type,
            validity_start: processItem.validity_start,
            location: processItem.location
        };
    });
    
    // 转换为格式化的JSON字符串
    return JSON.stringify(combinedData, null, 4);
}

export default {
    cleanJson,
    getSheetNames,
    extractSheetToJson,
    extractDownstreamProcess,
    extractDownstreamFlow,
    getProcessIds,
    loadExcel,
    extractUpstreamFlow,
    extractUpstreamProcess,
    extractUpstreamProcessTechnique,
    extractDownstreamDemand
};