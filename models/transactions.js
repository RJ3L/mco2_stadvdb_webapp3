const {node1, node2, node3, nodeUtils} = require('./nodes.js'); 

function sleep(milliseconds){
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

const transactionUtils = {
    doTransaction: async function (node, query){
        let connection
        try{
            connection = await nodeUtils.getConnection(node)
            await connection.beginTransaction()

            var [result, fields] = await connection.query(query)
            await connection.commit()
            console.log("Transaction Completed")
            return result
        } catch(error){
            if (connection){
                await connection.rollback()
            }
            throw error
        } finally {
            if (connection){
                await connection.release()
            }
        }
    },
    doMultiTransaction: async function (node, query){
        let connection
        try{
            connection = await nodeUtils.getConnection(node)
            var [result, fields] = await connection.query({
                sql: query,
                multipleStatements: true
            })
            console.log("Transaction Completed")
            return result
        } catch(error){
            console.log("Transaction Failed")
            console.log(error);
            if (connection){
                await connection.rollback()
            }
        } finally{
            if (connection){
                await connection.release()
            }
        }
    },
    executeSelect: async function (node, query, isolationLevel) {
        let connection;
        try {
            connection = await nodeUtils.getConnection(node);
            
            // 1. Set Isolation Level
            if (isolationLevel) {
                await connection.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
            }

            // 2. Start Transaction
            await connection.beginTransaction();

            // 3. Execute Query
            const [rows, fields] = await connection.query(query);

            // 4. Commit (End the transaction scope)
            await connection.commit();
            return rows;
        } catch (error) {
            if (connection) await connection.rollback();
            throw error;
        } finally {
            if (connection) await connection.release();
        }
    },
    executeUpdate: async function (node, query, isolationLevel, isDemoMode) {
        let connection;
        try {
            connection = await nodeUtils.getConnection(node);

            // 1. Set Isolation Level
            if (isolationLevel) {
                await connection.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
                console.log(`[Tx] Isolation set to: ${isolationLevel}`);
            }

            // 2. Start Transaction
            await connection.beginTransaction();

            // 3. Execute Update
            await connection.query(query);

            // 4. THE DEMO DELAY (The Trap)
            if (isDemoMode) {
                console.log(`[Tx] Demo Mode: Sleeping for 10s to allow interference...`);
                await sleep(10000); 
            }

            // 5. Commit
            await connection.commit();
            
            console.log("[Tx] Update Transaction Completed");
            return { affectedRows: 1, message: "Demo Update Complete" };

        } catch (error) {
            console.error("[Tx] Update Failed:", error.message);
            if (connection) await connection.rollback();
            throw error;
        } finally {
            if (connection) await connection.release();
        }
    },
    doDelayTransaction: async function (node, query){
        let connection
        try{
            connection = await nodeUtils.getConnection(node)
            await connection.beginTransaction()
            var [result, fields] = await connection.query(query)
            await sleep(5000)
            await connection.commit()
            console.log("Transaction Completed")
            return result
        } catch(error){
            if (connection){
                await connection.rollback()
            }
            throw error
        } finally{
            if (connection){
                await connection.release()
            }
        }
    },
}

module.exports = transactionUtils