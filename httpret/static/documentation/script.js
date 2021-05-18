"use strict";

function load_status_main(ev) {
    const ts1 = Date.now();
    const dom_ev = ev;
    const b = ev.target;
    b.classList.remove("loaded");
    b.classList.add("loading");
    b.value = b.dataset.btnLabel + "";
    const query = {
        hosts: "",
    };
    const fetch_init = {
        method: "get",
        /*headers: {
          retrieval_instance: document.getElementById("retrieval_instance").value,
        },
        body: JSON.stringify(query),*/
    };
    fetch(g_config.api_base + "node_status", fetch_init)
        .then(x => Promise.all([x.json(), Date.now()]))
        .then(g_config.ui_delay_test)
        .then(g_config.ui_delay_blink)
        .then(kk => {
            const js = kk[0];
            const ts2 = kk[1];
            if (false) {
                const response = document.getElementById("response");
                // Different ways to do the same thing:
                //response.querySelectorAll("*").forEach(n => n.remove());
                //response.innerHTML = "";
                response.textContent = "";
                while (response.firstChild) {
                    response.removeChild(response.lastChild);
                    response.lastChild.remove();
                }
                response.replaceChildren();
                //response.replaceChild();
                //JSON.stringify(js, null, 2);
                //for (let machine of js) {
                //  console.log(typeof(machine));
                //}
                const dat2 = js.hosts;
                sort_default(dat2);
                response.appendChild(render_retrieval_metrics_as_table(dat2));
                response.appendChild(render_host_memory_as_table(dat2));
                //response.appendChild(render_host_memStd_as_table(dat2));
                response.appendChild(render_host_bufferPools_as_table(dat2));
            }
            {
                let b = document.getElementById("load_status");
                b.innerHTML = "Loaded (" + (ts2 - ts1) + " ms)";
            }
            {
                let b = dom_ev.target;
                b.classList.remove("loading");
                b.classList.add("loaded");
                b.setAttribute("value", b.dataset.btnLabel);
            }
        });
}

var g_config = {
    api_base: "http://localhost:8059/api/4/",
    ui_delay_test: x => x,
    ui_delay_blink: x => new Promise(resolve => setTimeout(() => resolve(x), 50)),
    //ui_delay_blink: x => x,
};

function config_for_test() {
    g_config.api_base = "http://localhost:8059/api/4/";
}

function init() {
}

window.addEventListener("load", ev => {
    if (document.location.href.includes("8060")) {
        config_for_test();
    }
    init();
    const init_load_ele = document.getElementById("btn_load");
    if (init_load_ele != null) {
        init_load_ele.click();
    }
});
