const {node1, node2, node3, nodeUtils} = require('./nodes.js'); 
const transactionUtils = require('./transactions.js'); 


const syncUtils = {
    syncFragment: async function (nodeNum){
        console.log("Sync: Fragment " + nodeNum)
        if (await nodeUtils.pingNode(1) && await nodeUtils.pingNode(nodeNum)){
            let logs = []
            var baseQuery = (`SELECT MAX(log_id) AS idMax FROM log_table`)

            var maxMaster = await transactionUtils.doTransaction(1, baseQuery + '_' + nodeNum)
            var maxFrag = await transactionUtils.doTransaction(nodeNum, baseQuery)

            maxMaster = maxMaster[0].idMax ?? 0
            maxFrag = maxFrag[0]?.idMax ?? 0
            
            if (maxMaster > maxFrag){
                var masterQuery = (`SELECT * FROM log_table_` + nodeNum + ` WHERE log_id > ` + maxFrag)
                logs = await transactionUtils.doTransaction(1, masterQuery)

                let bulkQueries = " "
                bulkQueries += "START TRANSACTION; "
                for (i = 0; i < maxMaster - maxFrag; i++){
                    const log = logs[i]
                    if (log.action == "INSERT"){
                        const query = `REPLACE INTO node_` + nodeNum + ` (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (
                            '${log.tconst}', 
                            '${log.titleType}', 
                            '${log.primaryTitle}', 
                            '${log.originalTitle}',
                            ${log.isAdult},   
                            ${log.startYear}, 
                            ${log.endYear}, 
                            ${log.runtimeMinutes}, 
                            '${log.genres}'
                        );`
                        bulkQueries += query
                    } else if (log.action == "UPDATE"){
                        const query = `UPDATE node_` + nodeNum + ` SET 
                        titleType = '${log.titleType}',
                        primaryTitle = '${log.primaryTitle}',
                        originalTitle = '${log.originalTitle}',
                        isAdult = ${log.isAdult},
                        startYear = ${log.startYear},
                        endYear = ${log.endYearValue},
                        runtimeMinutes = ${log.runtimeMinutes},
                        genres = '${log.genres}'
                        WHERE tconst = '${log.tconst}';`
                    bulkQueries += query
                    } else if (log.action == "DELETE"){
                        const query = `DELETE FROM node_` + nodeNum +  ` WHERE tconst = '${log.tconst}';` 
                        bulkQueries += query
                    }
                }
                bulkQueries += "COMMIT;"
                let results = await transactionUtils.doMultiTransaction(nodeNum, bulkQueries);
                return results
            }
        }
    },
    syncMaster: async function (){
        console.log("Sync: Master Node")
        if (await nodeUtils.pingNode(1) && await nodeUtils.pingNode(2) && await nodeUtils.pingNode(3)){
            let node2Logs = []
            let node3Logs = []
            let combinedLogs = []

            var baseQuery = (`SELECT MAX(log_id) AS idMax FROM log_table`)
            var maxMaster2 = await transactionUtils.doTransaction(1, baseQuery + "_2")
            var maxMaster3 = await transactionUtils.doTransaction(1, baseQuery + "_3")
            var maxFrag2 = await transactionUtils.doTransaction(2, baseQuery)
            var maxFrag3 = await transactionUtils.doTransaction(3, baseQuery)
            
            maxMaster2 = maxMaster2[0]?.idMax ?? 0
            maxMaster3 = maxMaster3[0]?.idMax ?? 0
            maxFrag2 = maxFrag2[0]?.idMax ?? 0
            maxFrag3 = maxFrag3[0]?.idMax ?? 0

            if (maxMaster2 < maxFrag2){
                var node2Query =  (`SELECT * FROM log_table WHERE log_id > ` + maxMaster2)
                node2Logs = await transactionUtils.doTransaction(2, node2Query)
            } else if (maxMaster2 > maxFrag2){
                console.log("Alert: Node 2 needs to be synced")
            }

            if (maxMaster3 < maxFrag3){
                var node3Query = (`SELECT * FROM log_table WHERE log_id > ` + maxMaster3)
                node3Logs = await transactionUtils.doTransaction(3, node3Query)
            } else if (maxMaster3 > maxFrag3){
                console.log("Alert: Node 3 needs to be synced")
            }

            combinedLogs = node2Logs.concat(node3Logs)
            combinedLogs.sort((a, b) => b.action_time - a.action_time)

            let bulkQueries = " "
            bulkQueries += "SET @REPLICATOR_SYNC = 1; "
            bulkQueries += "START TRANSACTION; "
            
            for (i = 0; i < combinedLogs.length; i++){
                const log = combinedLogs[i]
                if (log.action == "INSERT"){
                    const query = `REPLACE INTO node_1 (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (
                        '${log.tconst}', 
                        '${log.titleType}', 
                        '${log.primaryTitle}', 
                        '${log.originalTitle}',
                        ${log.isAdult},   
                        ${log.startYear}, 
                        ${log.endYear}, 
                        ${log.runtimeMinutes}, 
                        '${log.genres}'
                    );`
                    bulkQueries += query
                } else if (combinedLogs[i].action == "UPDATE"){
                    const query = `UPDATE node_1 SET 
                        titleType = '${log.titleType}',
                        primaryTitle = '${log.primaryTitle}',
                        originalTitle = '${log.originalTitle}',
                        isAdult = ${log.isAdult},
                        startYear = ${log.startYear},
                        endYear = ${log.endYearValue},
                        runtimeMinutes = ${log.runtimeMinutes},
                        genres = '${log.genres}'
                        WHERE tconst = '${log.tconst}';`
                    bulkQueries += query
                } else if (log.action == "DELETE"){
                    const query = `DELETE FROM node_1 WHERE tconst = '${log.tconst}';` 
                    bulkQueries += query
                }
            }
            bulkQueries += "COMMIT;"
            let results = await transactionUtils.doMultiTransaction(1, bulkQueries);
            return results
        } else{
            console.log("Sync Node: Master Node is down")
        }
    }
}

module.exports = syncUtils