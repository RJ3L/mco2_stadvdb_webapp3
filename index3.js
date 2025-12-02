const express = require('express');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3002;
const db = require("./models/db.js");
const syncUtils = require("./models/sync.js");
const { nodeUtils } = require("./models/nodes.js");
const cors = require('cors');
const {demoCaseCrash, demoCaseRecovery} = require("./tests/recovery.js")
const Title = require("./models/title.js")
const dbNode3 = require("./models/db_node3.js");

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));


app.get('/', (req, res) => {
    res.send('Hello from the Node.js backend!');    
});

app.get('/api/pingNode/:id', async (req, res) => {
    const node = parseInt(req.params.id);
    try {
        const alive = await nodeUtils.pingNode(node);
        res.status(200).json({ alive: alive });
    } catch (error) {
        res.status(500).json({ alive: false, error: error.message });
    }
});

app.post('/api/read', async (req, res) => {
    const { id, isolationLevel } = req.body; 

    try {
        console.log(`Reading Entry: ${id} | Isolation: ${isolationLevel}`);
        const query = `WHERE tconst = '${id}'`;
        
        const result = await db.selectQuery(query, "LIMIT 1", 1900, 2100, 1, isolationLevel);
        
        res.status(200).json({ message: 'Read successful', result: result }); 
    } catch (error) {
        console.error("Read Error:", error);
        res.status(500).json({ message: 'Read failed', error: error.message });
    }
});

app.post('/api/insert', async (req, res) => {
    const {
        tconst, titleType, primaryTitle, originalTitle, isAdult, 
        startYear, endYear, runtimeMinutes, genres, 
        isolationLevel, isDemoMode 
    } = req.body;
    const insertQuery = `VALUES ('${tconst}', '${titleType}', '${primaryTitle}', '${originalTitle}', ${isAdult}, ${startYear}, ${endYear}, ${runtimeMinutes}, '${genres}');`;

    try {
        console.log(`Inserting... Demo: ${isDemoMode}`);
        const result = await db.insertQuery(insertQuery, parseInt(startYear), 1, isolationLevel, isDemoMode);
        res.status(200).json({ message: 'Insert successful', result: result }); 
    } catch (error) {
        res.status(500).json({ message: 'Insert failed', error: error.message }); 
    }
});

app.post('/api/update', async (req, res) => {
    const {
        tconst, titleType, primaryTitle, originalTitle, isAdult, 
        startYear, endYear, runtimeMinutes, genres,
        isolationLevel, isDemoMode
    } = req.body;
    const updateQuery = `${titleType}, ${primaryTitle}, ${originalTitle}, ${isAdult}, ${startYear}, ${endYear}, ${runtimeMinutes}, ${genres}`;
    
    try {
        console.log(`Updating Query... Demo: ${isDemoMode} | Isolation: ${isolationLevel}`);
        const result = await db.updateQuery(
            updateQuery, 
            tconst, 
            parseInt(startYear), 
            1,
            isolationLevel, 
            isDemoMode
        );
        
        res.status(200).json({ message: 'Update successful', result: result }); 
    } catch (error) {
        res.status(500).json({ message: 'Update failed', error: error.message }); 
    }
});

app.post('/api/delete', async (req, res) => {
    const { id, year, isolationLevel, isDemoMode } = req.body;

    try {
        console.log(`Deleting... Demo: ${isDemoMode}`);
        const result = await db.deleteQuery(id, parseInt(year), 1, isolationLevel, isDemoMode);
        res.status(200).json({ message: 'Delete successful', result: result }); 
    } catch (error) {
        res.status(500).json({ message: 'Delete failed', error: error.message }); 
    }
});

app.get('/api/database', async (req, res) => {
    try {
        const result = await db.selectQuery("", "ORDER BY tconst ASC LIMIT 100", 1000, 2100, 1);
        
        if (result) {
            res.status(200).json(result); 
        } else {
            res.status(200).json([]); 
        }
    } catch (error) {
        console.error("Error fetching table data:", error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sync', async (req, res) => {
    try {
        console.log("[Sync] Starting manual synchronization...");
        
        // Master pulls new data from Nodes 2 & 3
        await syncUtils.syncMaster();
        
        // Nodes 2 & 3 pull latest data from Master
        await syncUtils.syncFragment(2);
        await syncUtils.syncFragment(3);
        
        console.log("[Sync] Synchronization Complete.");
        res.status(200).json({ message: 'Sync Complete' });
    } catch (error) {
        console.error("Sync error:", error);
        res.status(500).json({ message: 'Sync failed', error: error.message });
    }
});

app.post('/test/recovery', async (req, res) => {
    const {testCase, fragNode, action, data} = req.body;
    if (!(await nodeUtils.pingNode(1) && await nodeUtils.pingNode(fragNode))){
        return {status: -1, error: "The nodes are not available."}
    }
    const newTitle = new Title(data.tconst, data.titleType, data.primaryTitle, data.originalTitle, data.isAdult, data.startYear, data.endYear, data.runtimeMinutes, data.genres)
    try{
        if (testCase == '1'){
            console.log("[Recovery] Case 1")
            let {result, error} = await demoCaseCrash(parseInt(fragNode), 1, action, newTitle)
            res.json({status: result, message: error })
        } else if (testCase == '2'){
            console.log("[Recovery] Case 2")
            let {result, error} = await demoCaseRecovery(parseInt(fragNode), 1, action, newTitle)
            res.json({status: result, message: error })
        } else if (testCase == '3'){
            console.log("[Recovery] Case 3")
            let {result, error} = await demoCaseCrash(1, parseInt(fragNode), action, newTitle)
            res.json({status: result, message: error })
        } else{
            console.log("[Recovery] Case 4")
            let {result, error} = await demoCaseRecovery(1, parseInt(fragNode), action, newTitle)
            res.json({status: result, message: error })
        }
    } catch (error){
        console.error("[Error] ", error);
        res.status(500).json({ message: 'Test failed: ', error: error.message });
    }
});


app.post('/api/ConcurrencyInsert', async (req, res) => {
    try {
        const result = await dbNode3.insertQuery(req.body);
        res.status(200).json({ message: 'Insert successful', result: result });
    } catch (error) {
        console.log(error);
        res.status(500).json({ message: 'Insert failed', error: error.message });
    }
});

app.post('/api/ConcurrencyUpdate', async (req, res) => {
    try {
        const result = await dbNode3.updateQuery(req.body);
        res.status(200).json({ message: 'Update successful', result: result });
    } catch (error) {
        console.log(error);
        res.status(500).json({ message: 'Update failed', error: error.message });
    }
});

app.post('/api/ConcurrencyDelete', async (req, res) => {
    try {
        return await dbNode3.deleteQuery(req.body);
    } catch (error) {
        console.log(error);
    }
});

app.post('/api/ConcurrencyRead', async (req, res) => {
    try {
        result = await dbNode3.getSingleTitle(req.body);
        //console.log("index", result);
        if (result) {
             res.status(200).json(result); 
        } else {
             res.status(404).json({ message: "Record not found" });
        }
    } catch (error) {
        console.log(error);
    }
});

app.post('/api/ConcurrencySync', async (req, res) => {
    try {
        const result = await dbNode3.syncData();
        res.status(200).json({ message: 'Sync Complete', result: result });
    } catch (error) {
        console.log(error);
        res.status(500).json({ message: 'Sync failed', error: error.message });
    }
});

app.post('/api/ConcurrencyIsolationLevel', async (req, res) => {
    try {
        const result1 = await dbNode3.setIsolationLevel(req.body);
        res.status(200).json({ message: 'Isolation Level Set ' + result1, result: result1 });
    } catch (error) {
        console.log(error);
        res.status(500).json({ message: 'Test failed: ' + error.message });
    }
})

app.listen(PORT, async () => {
    console.log(`Server listening on port ${PORT}`);
        await dbNode3.getNodeInfo()
        console.log("READ")
});