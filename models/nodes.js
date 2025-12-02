const mysql = require('mysql2/promise');
const dotenv = require(`dotenv`).config();


const baseConfig = {
    host: process.env.DB_HOST || 'ccscloud.dlsu.edu.ph',
    user: process.env.DB_USER || 'root', 
    password: process.env.DB_PASS || 'g27d94sHTDWRxQ38eSpAtrYE',
    waitForConnections: true,
    multipleStatements: true,
    connectionLimit: 10,
    maxIdle: 10, 
    idleTimeout: 60000, 
    queueLimit: 0
}
const node1 = mysql.createPool({
    port: process.env.DB_PORT_01 || 60745,
    database: 'server0_db',
    ...baseConfig
});

const node2 = mysql.createPool({
    port: process.env.DB_PORT_02 || 60746,
    database: 'server1_db',
    ...baseConfig
});

const node3 = mysql.createPool({
    port: process.env.DB_PORT_03 || 60747,
    database: 'server2_db',
    ...baseConfig
});

const nodes = [node1, node2, node3];

const nodeUtils = {
    pingNode: async function (n){
        try {
            const [rows, fields] = await nodes[n - 1].query(`SELECT 1`);
            return true;
        }
        catch (err) {
            console.log(`Error: Node ${n} is not available`);
            return false;
        }
    },
    pingAllNodes: async function (){
        const node2Alive = await nodeUtils.pingNode(2);
        const node3Alive = await nodeUtils.pingNode(3);
        return {
            node2Alive: node2Alive,
            node3Alive: node3Alive
        };
    },
    getConnection: async function(n){
        switch (n) {
            case 1: return await node1.getConnection();
            case 2: return await node2.getConnection();
            case 3: return await node3.getConnection();
        }
    }
}

module.exports = {
    node1: node1,
    node2: node2,
    node3: node3,
    nodeUtils
}