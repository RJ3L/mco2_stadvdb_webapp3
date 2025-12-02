// Reference: https://developer.mozilla.org/en-US/docs/Web/API/Document/getElementById
const getValue = (id) => {
    const el = document.getElementById(id);
    return el ? el.value : '';
}

// NEW: Helper to get Isolation Level and Demo Mode
const getConcurrencySettings = () => {
    const isoEl = document.getElementById('isolationLevel');
    const demoEl = document.getElementById('demoMode');
    
    return {
        isolationLevel: isoEl ? isoEl.value : 'READ COMMITTED', // Default
        isDemoMode: demoEl ? demoEl.checked : false             // Default false
    };
}

document.addEventListener('DOMContentLoaded', () => {
    fetchDatabaseData();
});

async function fetchDatabaseData() {
    const tableBody = document.getElementById('database-body');
    if(!tableBody) return;

    try {
        const response = await fetch('/api/database');
        const data = await response.json();

        tableBody.innerHTML = '';
        const rows = Array.isArray(data) ? data : (data.result || []);

        if (rows.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="8" style="text-align:center;">No records found</td></tr>';
            return;
        }

        rows.forEach(row => {
            if (!row.tconst) return;
            
            const tr = document.createElement('tr');
            tr.innerHTML = `
            <td>${row.tconst}</td> 
            <td>${row.titleType}</td>
            <td>${row.primaryTitle}</td>
            <td>${row.originalTitle}</td>
            <td>${row.isAdult}</td>
            <td>${row.startYear}</td>
            <td>${row.endYear || 'N/A'}</td>
            <td>${row.runtimeMinutes}</td>
            <td>${row.genres}</td>
            `;
            tableBody.appendChild(tr);
        });
    } catch (error) {
        console.error('Error fetching database data:', error.message);
    }
}

function populateForms(row) {
    // Fill Insert Form
    setValue('insert_id', row.tconst);
    setValue('insert_TitleType', row.titleType);
    setValue('insert_PrimaryName', row.primaryTitle);
    setValue('insert_OriginalName', row.originalTitle);
    setValue('insert_Adult', row.isAdult);
    setValue('insert_StartYear', row.startYear);
    setValue('insert_EndYear', row.endYear);
    setValue('insert_RunTime', row.runtimeMinutes);
    setValue('insert_Genre', row.genres);

    // Fill Update Form
    setValue('update_id', row.tconst);
    setValue('update_titleType', row.titleType);
    setValue('update_PrimaryName', row.primaryTitle);
    setValue('update_OriginalName', row.originalTitle);
    setValue('update_Adult', row.isAdult);
    setValue('update_StartYear', row.startYear);
    setValue('update_EndYear', row.endYear);
    setValue('update_RunTime', row.runtimeMinutes);
    setValue('update_Genre', row.genres);

    // Fill Delete/Read Form
    setValue('delete_id', row.tconst);
    setValue('delete_year', row.startYear);
    setValue('read_id', row.tconst);
    
    console.log("Form populated with:", row.tconst);
}

async function handleInsert(){
    const data = {
       tconst: getValue('insert_id'),
       titleType: getValue('insert_TitleType'),
       primaryTitle: getValue('insert_PrimaryName'), 
       originalTitle: getValue('insert_OriginalName'), 
       isAdult: getValue('insert_Adult'), 
       startYear: getValue('insert_StartYear'), 
       endYear: getValue('insert_EndYear'), 
       runtimeMinutes: getValue('insert_RunTime'),
       genres: getValue('insert_Genre'), 
    };
    
    const { isolationLevel, isDemoMode } = getConcurrencySettings();
    const payload = { ...data, isolationLevel, isDemoMode };

    try {
        const response = await fetch('/api/insert', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const result = await response.json();
        alert("Insert: " + result.message);
        fetchDatabaseData();
    } catch (error) {
        console.error(error.message);
    }
}

async function handleUpdate(){
    const data = {
       tconst: getValue('update_id'),
       titleType: getValue('update_titleType'),
       primaryTitle: getValue('update_PrimaryName'), 
       originalTitle: getValue('update_OriginalName'), 
       isAdult: getValue('update_Adult'), 
       startYear: getValue('update_StartYear'), 
       endYear: getValue('update_EndYear'), 
       runtimeMinutes: getValue('update_RunTime'),
       genres: getValue('update_Genre'), 
    };

    const { isolationLevel, isDemoMode } = getConcurrencySettings();

    const payload = {
        ...data,
        isolationLevel,
        isDemoMode
    };

    console.log("Sending Update with:", payload);

    try {
        const response = await fetch('/api/update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const result = await response.json();
        alert("Update: " + result.message);
        fetchDatabaseData();
    } catch (error) {
        console.error(error.message);
        alert("Update Failed");
    }
} 

async function handleDelete(){
    const data = {
        id: getValue('delete_id'), 
        year: getValue('delete_year')
    };

    const { isolationLevel, isDemoMode } = getConcurrencySettings();
    const payload = { ...data, isolationLevel, isDemoMode };

    try {
        const response = await fetch('/api/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const result = await response.json();
        alert("Delete: " + result.message);
        fetchDatabaseData();
    } catch (error) {
        console.error(error.message);
    }
}

async function handleRead(){
    const { isolationLevel } = getConcurrencySettings();
    const data = {
        id: getValue('read_id'),
        year: getValue('read_year'),
        isolationLevel: isolationLevel // Send Isolation Level to Backend
    };

    try {
        const response = await fetch('/api/read', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });
        const result = await response.json();
        
        if(result.result && result.result.length > 0){
             alert("Record Found (Isolation: " + isolationLevel + "):\n" + JSON.stringify(result.result[0], null, 2));
        } else {
             alert("No record found");
        }
    } catch (error) {
        console.error(error.message);
    }
}

async function checkNodeStatus(node){   
    const element = document.getElementById(`node-${node}`);
    try {
        const response = await fetch(`/api/pingNode/${node}`);
        const result = await response.json();
        if (result.alive) {
            element.textContent = `Reachable`;
            element.style.color = 'green';
        } else {
            element.textContent = `Not Reachable`;
            element.style.color = 'red';
        }
    } catch (error) {
        console.error(`Error checking status of Node ${node}:`, error);
        element.textContent = `Error checking Node ${node}`;
        element.style.color = 'orange';
    }
}

async function handleSync() {
    console.log("Syncing nodes...");
    
    try {
        const response = await fetch('/api/sync', { method: 'POST' });
        const result = await response.json();
        
        alert("System Message: " + result.message);
        
        // Auto-refresh the table to show the synced data
        fetchDatabaseData(); 
    } catch (error) {
        console.error(error);
        alert("Sync failed. Check console for details.");
    }
}
