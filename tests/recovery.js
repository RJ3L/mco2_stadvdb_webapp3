const { nodeUtils } = require('../models/nodes.js')
const syncUtils = require('../models/sync.js')
const Title = require('../models/title.js')
/*
 * Case 1: When the central node attempts to replicate an insert or update from a fragment node.
 *      fragmentNode -> sourceNode
 *      centralNode (1) -> targetNode
 * Case 3: When a fragment node attempts to replicate an insert or update from the central node.
 *      centralNode (1) -> sourceNode
 *      fragmentNode -> targetNode
 */

async function demoCaseCrash(sourceNode, targetNode, action, newTitle){
    console.log("[Recovery] Case 1 & 3: Target node attempts to replicate a write transaction from the source node")
    let sourceConn = await nodeUtils.getConnection(sourceNode)
    let targetConn = null
    let logTable = "log_table"
    let flag = 0
    let errorString = "Success: "

    if (sourceNode == 1){
        logTable += `_${targetNode}`
    }

    try{
        console.log("   [STEP 1] Source node starts a write transaction and commits.")
        await sourceConn.beginTransaction()

        let query = `
            SET @NODE_2_ALIVE = 0;
            SET @NODE_3_ALIVE = 0;
            REPLACE INTO node_${sourceNode} (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

        let values = [
            newTitle.tconst,
            newTitle.titleType,
            newTitle.pTitle,
            newTitle.oTitle,
            newTitle.isAdult,
            newTitle.startYear,
            newTitle.endYear,
            newTitle.runtime,
            newTitle.genres
        ];
        await sourceConn.query(query, values)
        await sourceConn.commit()
        sourceConn.release()

        console.log("   [STEP 2] Target node recreates the transaction.")
            if (action == "UPDATE"){
            query = `
                SET @NODE_2_ALIVE = 0;
                SET @NODE_3_ALIVE = 0;
                UPDATE node_${targetNode} SET titleType=?, primaryTitle=?, originalTitle=?, isAdult=?, startYear=?, endYear=?, runtimeMinutes=?, genres=? 
                WHERE tconst=?`
            values = [
                newTitle.titleType, newTitle.pTitle, newTitle.oTitle, newTitle.isAdult,
                newTitle.startYear, newTitle.endYear, newTitle.runtime, newTitle.genres, newTitle.tconst
            ]
        } else{
            query = `
                SET @NODE_2_ALIVE = 0;
                SET @NODE_3_ALIVE = 0;
                INSERT INTO node_${targetNode} (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
            values = [
                newTitle.tconst, newTitle.titleType, newTitle.pTitle, newTitle.oTitle, newTitle.isAdult,
                newTitle.startYear, newTitle.endYear, newTitle.runtime, newTitle.genres
            ]
        }
        targetConn = await nodeUtils.getConnection(targetNode)
        await targetConn.beginTransaction()
        await targetConn.query(query, values)

        console.log("   [STEP 3] Throw an error to simulate a crash before the target node can commit.")
        throw new Error("[Error: Connection Closed] Target node disconnected before commit.")
    } catch(error){
        console.log(`   [Error] ${error.message}`)
        console.log("   [STEP 4] Destroy the connection.")
        targetConn.destroy()
    }

    console.log("   [STEP 5] Verify that the target node is clean.")
    targetConn = await nodeUtils.getConnection(targetNode)
    const [rows] = await targetConn.query(`SELECT * from node_${targetNode} WHERE tconst = ?`, [newTitle.tconst])
    targetConn.release()
    if (rows.length == 0){
        console.log("       [Success] No entry has been inserted.")
    } else if (action == "UPDATE" && rows[0].primaryTitle != newTitle.pTitle){
        console.log("       [Success] The entry has not been updated.")
    } else{
        console.log(rows[0].primaryTitle)
        console.log(newTitle.pTitle)
        console.log("       [Fail] The write transaction was successful.")
        flag = 1
        errorString = "Fail: The write transaction in the target node proceeded. "
    }

    console.log("   [STEP 6] Verify that a log has been created in the source node.")
    sourceConn = await nodeUtils.getConnection(sourceNode)
    const [logs] = await sourceConn.query(`SELECT * FROM ${logTable} WHERE tconst = ?`, [newTitle.tconst])
    sourceConn.release()
    if (logs.length > 0){
        console.log(`       [Success] A log has been created with the ID of ${logs[0].log_id}.`)
        return {status: flag, log: logs[0], error: errorString + `A log has been created with the ID of ${logs[0].log_id}.`}
    } else {
        console.log("       [Fail] No log has been created.")
        flag += 2
        return {status: flag, log: null, error: errorString + "No log has been created."}
    }
}

/*
 * Case 2: When the central node comes back online and missed some write transactions.
 *      fragmentNode -> sourceNode
 *      centralNode (1) -> targetNode
 * Case 4: When a fragment node comes back online and missed some write transactions.
 *      centralNode (1) -> sourceNode
 *      fragmentNode -> targetNode
 */
async function demoCaseRecovery(sourceNode, targetNode, action, newTitle){
    console.log("[Recovery] Case 2 & 4: Target node comes back online and missed some transactions.")
    let sourceConn = await nodeUtils.getConnection(sourceNode)
    let targetConn = null
    console.log("   [STEP 1] Source node starts a write transaction and commits.")
    await sourceConn.beginTransaction()
    let query = ''
    let values = ''
    if (action == "UPDATE"){
        query = `
            SET @NODE_2_ALIVE = 0;
            SET @NODE_3_ALIVE = 0;
            UPDATE node_${sourceNode} 
                SET titleType=?, primaryTitle=?, originalTitle=?, isAdult=?, startYear=?, endYear=?, runtimeMinutes=?, genres=? 
                WHERE tconst=?`
        values = [
            newTitle.titleType, newTitle.pTitle, newTitle.oTitle, newTitle.isAdult,
            newTitle.startYear, newTitle.endYear, newTitle.runtime, newTitle.genres, newTitle.tconst
        ]
    } else{
        query = `
            SET @NODE_2_ALIVE = 0;
            SET @NODE_3_ALIVE = 0;
            INSERT INTO node_${sourceNode} (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
        values = [
            newTitle.tconst, newTitle.titleType, newTitle.pTitle, newTitle.oTitle, newTitle.isAdult,
            newTitle.startYear, newTitle.endYear, newTitle.runtime, newTitle.genres
        ]
    }
    await sourceConn.query(query, values)
    await sourceConn.commit()

    console.log("   [STEP 2] Verify that that the transaction was successful.")
    let [rows] = await sourceConn.query(`SELECT * FROM node_${sourceNode} WHERE tconst = ?`, newTitle.tconst)
    sourceConn.release()
    if (rows.length > 0 && rows[0].primaryTitle == newTitle.pTitle){
        console.log("       [Success] The transaction was successful.")
    } else{
        console.log("       [Fail] The transaction failed.")
        return {status: 1, error: "Fail: The write transaction in the source node did not succeed."}
    }

    console.log("   [STEP 3] Verify that the target node has not been updated.")
    targetConn = await nodeUtils.getConnection(targetNode)
    let [mRows] = await targetConn.query(`SELECT * FROM node_${targetNode} WHERE tconst = ?`, newTitle.tconst)
    targetConn.release()
    if (action == "UPDATE" && mRows[0].primaryTitle != newTitle.pTitle){
        console.log("       [Success] The entry in the target node has not been modified.")
    } else if (mRows.length == 0){
        console.log("       [Success] No entry has been inserted in the target node.")
    } else{
        console.log("       [Fail] The target node has been modified.")
        return {status: 2, error: "Fail: The target node has been modified before syncing."}
    }
    if (targetNode == 1){
        console.log("   [STEP 4] Call syncMaster to replicate the transaction.")
        await syncUtils.syncMaster()
    } else{
        console.log("   [STEP 4] Call syncFrag to replicate the transaction.")
        await syncUtils.syncFragment(targetNode)
    }

    console.log("   [STEP 5] Verify that the targetNode has been synced.")
    
    let [cRows] = await targetConn.query(`SELECT * FROM node_${targetNode} WHERE tconst = ?`, newTitle.tconst)
    targetConn.release()
    if (cRows.length > 0 && cRows[0].primaryTitle == newTitle.pTitle){
        console.log("       [Success] The target node has been updated.")
        return {status: 1, error: "Success: The target node has been updated."}
    } else{
        console.log("       [Fail] The target node has not been synced.")
        return {status: 3, error: "Fail: The target node was not synced with the source node."}
    }
}
module.exports = {
    //demoCaseCrash: demoCaseCrash,
    //demoCaseRecovery: demoCaseRecovery
}
const TEST_TYPE = process.argv[2]; 
const NODE_ARG = process.argv[3];
const ID_ARG   = process.argv[4];
const NEW_TITLE = process.argv[5]; 
const NODE = NODE_ARG ? parseInt(NODE_ARG) : 1; 

(async () => {
    if (!TEST_TYPE || NODE == 1 ||!ID_ARG){
        console.log("Please provide arguments: <TEST_NUM> <NODE_ID> <TITLE_ID> <NEW_TITLE>");
        process.exit();
    }
    const testTitle = new Title(ID_ARG, "MOVIE", "Test Demo", "Demo Test", 0, 2000, 2001, 120, "Horror,Thriller")
    try {
        switch(TEST_TYPE){
            case '1': await demoCaseCrash(NODE, 1, "INSERT", testTitle); break;
            case '2': await demoCaseRecovery(NODE, 1, "INSERT", testTitle); break;
            case '3': await demoCaseCrash(1, NODE, "INSERT", testTitle); break;
            case '4': await demoCaseRecovery(1, NODE, "INSERT", testTitle); break;
            default: console.log("Unknown Test Type");
        }
    } catch (e){
        console.error("Test Error:", e.message);
    }
    process.exit();
})();