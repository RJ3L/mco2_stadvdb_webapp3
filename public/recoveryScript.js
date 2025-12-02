function openRecoveryModal(testCase){
    const title = document.getElementById('testCase')
    const modal = document.getElementById('recoveryModal')
    modal.style.display = "flex"
    if (testCase == 1){
        title.innerText = "CASE 1: When the central node attempts to replicates a write transaction in the fragment node."
    } else if(testCase == 2){
        title.innerText = "CASE 2: When the central node comes back online and missed some write transactions."
    } else if(testCase == 3){
        title.innerText = "CASE 3: When a fragment node attempts to replicate a write transaction in the central node."
    } else{
        title.innerText = "CASE 4: When a fragment node comes back online and missed some write transactions."
    }

    const submitBtn = document.getElementById('recoverySubmitBtn');
    submitBtn.onclick = function() {
        submitRecoveryTest(testCase);
    };
}
function closeRecoveryModal(){
    document.getElementById('recoveryModal').style.display = "none"
}
window.onclick = function(event){
    const modal = document.getElementById('recoveryModal')
    if (event.target == modal){
        modal.style.display = 'none'
    }
}

/* Test */
async function submitRecoveryTest(testCase){
    const action = getValue('rec_transaction')
    const fragNode = getValue('rec_fragment')
    const data = {
        tconst: getValue('rec_tconst'),
        titleType: getValue('rec_titleType'),
        primaryTitle: getValue('rec_primaryTitle'),
        originalTitle: getValue('rec_originalTitle'),
        isAdult: getValue('rec_isAdult') ? 1:0,
        startYear: getValue('rec_startYear'),
        endYear: getValue('rec_endYear'),
        runtimeMinutes: getValue('rec_runtimeMinutes'),
        genres: getValue('rec_genres')
    }

    const payload = {testCase, fragNode, action, data}
    try {
        const response = await fetch('/test/recovery', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        console.log("hey")
        alert("[Recovery Result] " + result.message);
        closeRecoveryModal() 
    } catch (error) {
        console.error(error.message);
    }
}