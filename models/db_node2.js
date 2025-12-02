import {node1, node2, node3, nodeUtils} from './nodes.js'; 
import transactionUtils from './transactions.js'; 

let isolationLevel = "REPEATABLE READ";

let node2Logs = [];
let numRows = 0;

let syncList = [];
let syncCount = 0;

let UncommitedLog = [];


export async function getNodeInfo() {
    try {
        let connection = await nodeUtils.getConnection(1);
        const [rows] = await connection.query('SELECT * FROM node_1');
        node2Logs = rows;
        UncommitedLog = [rows];
    } catch (error) {
        let connection1 = await nodeUtils.getConnection(2);
        let connection2 = await nodeUtils.getConnection(3);
        const [results1] = await connection1.query('SELECT * FROM node_2');
        const [results2] = await connection2.query('SELECT * FROM node_3');
        node2Logs = [...results1, ...results2];
        UncommitedLog = [...results1, ...results2];
    }
    if(syncList.length) {
        applySyncList();
    }
    numRows = node2Logs.length;
    console.log("Read Done");
    return node2Logs;
}

export async function applySyncList() {
    for(let i = 0; i < syncList.length; i++) {
        if(syncList[i].type == "INSERT") {
            insertQuery(syncList[i].data, true);
        } else if(syncList[i].type == "UPDATE") {
            updateQuery(syncList[i].data, true);
         } else if(syncList[i].type =="DELETE") {    
            deleteQuery(syncList[i].data, true);
        }
    }
}

export async function getSingleTitle(data) {
    if (isolationLevel == "READ COMMITTED") {
        await getNodeInfo();
    }

    if (isolationLevel == "READ UNCOMMITTED") {
        console.log("UNCOMMITTED");
        const index = UncommitedLog.findIndex(log => String(log.tconst) === String(data.id));
        const result = UncommitedLog[index];
        return result;
    } else {
        console.log("COMMITTED")
        const index = node2Logs.findIndex(log => String(log.tconst) === String(data.id));
        const result = node2Logs[index];
        return result;
    }
}


export async function updateQuery(data, isReplay = false) { 
    if (syncList.length > 0 && isolationLevel == "SERIALIZABLE") {
        throw new Error("Required Commit First before starting another transaction");
    }
    if (isolationLevel == "READ COMMITTED" && !isReplay) {
        await getNodeInfo();
    }
        if (UncommitedLog.length === 0) {
            console.warn("Warning: UncommitedLog is empty. Did you run getNodeInfo() first?");
            return;
        }

        const index = UncommitedLog.findIndex(log => String(log.tconst) === String(data.tconst));

        if (index !== -1) {
            console.log("Found item to update:", UncommitedLog[index]);
            UncommitedLog[index] = { ...UncommitedLog[index], ...data }; 

            console.log(`Successfully updated tconst: ${data.tconst}`);
            console.log("New state:", UncommitedLog[index]);
        } else {
            console.log(`tconst not found: ${data.tconst}`);
        }
    if (!isReplay) {
        syncList[syncCount] = {type:"UPDATE", data: data};
        syncCount++;
    }
    
}

export async function insertQuery(insertData, isReplay = false) {
    if (syncList.length > 0 && isolationLevel == "SERIALIZABLE") {
        throw new Error("Required Commit First before starting another transaction");
    }
    if (isolationLevel == "READ COMMITTED" && !isReplay) {
        await getNodeInfo();
    }
    UncommitedLog.push(insertData);
    console.log(`Successfully inserted tconst: ${insertData.tconst}`);
    syncList[syncCount] = {type:"INSERT", data: insertData};
    syncCount++;
    console.log(syncList[0]);

    if (!isReplay) {
            syncList[syncCount] = {type:"INSERT", data: insertData};
            syncCount++;
    }
}

export async function deleteQuery(tconst, isReplay = false) { 
    if (syncList.length > 0 && isolationLevel == "SERIALIZABLE") {
        throw new Error("Required Commit First before starting another transaction");
    }
    if (isolationLevel == "READ COMMITTED" && !isReplay) {
        await getNodeInfo();
    }
        const index = UncommitedLog.findIndex(log => String(log.tconst) === String(tconst.id));
        
        if (index !== -1) {
            UncommitedLog.splice(index, 1);
            console.log(`Successfully deleted tconst: ${tconst.id}`);
        } else {
            console.log(`tconst not found for deletion: ${tconst.id}`);
        }
        if (!isReplay) {
            syncList[syncCount] = {type:"DELETE", data: tconst};
            syncCount++;
        }

}

export async function setIsolationLevel(level) {
        isolationLevel = level.isolationLevel;
        return level.isolationLevel;
}

export async function syncData() {
    for(let i = 0; i < syncList.length; i++) {
        let currentItem = syncList[i];
        if(syncList[i].type == "INSERT") {
            try {
                let baseQuery = "INSERT INTO "
                let tableQuery = " (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) "
                let valuesQuery = "VALUES ('" + currentItem.data.tconst + "','" + 
                    currentItem.data.titleType + "','" + 
                    currentItem.data.primaryTitle + "','" + 
                    currentItem.data.originalTitle + "'," + 
                    currentItem.data.isAdult + "," + 
                    currentItem.data.startYear + "," + 
                    (currentItem.data.endYear === null ? "NULL" : currentItem.data.endYear) + "," + 
                    currentItem.data.runtimeMinutes + ",'" + 
                    currentItem.data.genres + "');";
                let query = `
                START TRANSACTION;
                ${baseQuery} node_1 ${tableQuery} ${valuesQuery}
                COMMIT;`
                let result = await transactionUtils.doTransaction(1, query);
            } catch (error) {
                console.log(error);
            }
        } else if(syncList[i].type == "UPDATE") {
            try {
                let updateClause = " SET " + 
                "titleType = '" + currentItem.data.titleType  + "', " + 
                "primaryTitle = '" + currentItem.data.primaryTitle + "', " + 
                "originalTitle = '" + currentItem.data.originalTitle + "', " + 
                "isAdult = '" + currentItem.data.isAdult + "', " + 
                "startYear = '" + currentItem.data.startYear + "', " + 
                "endYear = " + (currentItem.data.endYear === null ? "NULL" : currentItem.data.endYear) + ", " + 
                "runtimeMinutes = '" + currentItem.data.runtimeMinutes + "', " + 
                "genres = '" + currentItem.data.genres + "' " + 
                "WHERE tconst = '" + currentItem.data.tconst + "';";

                let updateQuery = `
                START TRANSACTION;
                UPDATE node_1 ${updateClause}
                COMMIT;
                `
                let result = await transactionUtils.doMultiTransaction(1, updateQuery);
            } catch (error) {
                console.log(error)
            }
         } else if(syncList[i].type =="DELETE") {    
            let baseQuery = "DELETE FROM "
            let tableQuery = " WHERE tconst = '" + currentItem.data.id + "';"
            let deleteQuery = `
                START TRANSACTION;
                ${baseQuery} node_1 ${tableQuery}
                COMMIT;
            `
            let result = await transactionUtils.doMultiTransaction(1, deleteQuery);
        }
    }
    syncList = [];
    syncCount = 0;
    await getNodeInfo();
}