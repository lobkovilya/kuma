import{L as x}from"./LoadingBox-1c204d25.js";import{O as z,a as Z,b as I}from"./OnboardingPage-d8b67645.js";import{j as k,l as T,k as A,g as O,_ as M,h as N}from"./RouteView.vue_vue_type_script_setup_true_lang-d7cfbc23.js";import{_ as V}from"./RouteTitle.vue_vue_type_script_setup_true_lang-d610bc33.js";import{d as L,s as r,M as U,o as a,a as C,b as i,i as l,e as m,h as e,l as n,f as c,g as S,p as B,n as $}from"./index-6fad8e68.js";import"./kongponents.es-208ec824.js";const E=u=>(B("data-v-40900992"),u=u(),$(),u),K=E(()=>n("p",{class:"mb-4 text-center"},`
            A zone requires both the zone control plane and zone ingress. On Kubernetes, you run a single command to create both resources. On Universal, you must create them separately.
          `,-1)),R={class:"mb-4 text-center"},D=["href"],P={class:"status-box mt-4"},G={key:0,class:"status--is-connected","data-testid":"zone-connected"},j={key:1,class:"status--is-disconnected","data-testid":"zone-disconnected"},q={class:"status-box mt-4"},H={key:0,class:"status--is-connected","data-testid":"zone-ingress-connected"},Q={key:1,class:"status--is-disconnected","data-testid":"zone-ingress-disconnected"},Y={key:0,class:"status-loading-box mt-4"},w=1e3,F=L({__name:"MultiZoneView",setup(u){const p=k(),f=T(),{t:y}=A(),s=r(!1),o=r(!1),d=r(null),_=r(null);U(function(){h(),b()}),v(),g();async function v(){try{const{total:t}=await p.getZones();s.value=t>0}catch(t){s.value=!1,console.error(t)}finally{s.value||(h(),d.value=window.setTimeout(v,w))}}async function g(){try{const{total:t}=await p.getAllZoneIngressOverviews();o.value=t>0}catch(t){o.value=!1,console.error(t)}finally{o.value||(b(),_.value=window.setTimeout(g,w))}}function h(){d.value!==null&&window.clearTimeout(d.value)}function b(){_.value!==null&&window.clearTimeout(_.value)}return(t,J)=>(a(),C(M,null,{default:i(()=>[l(V,{title:m(y)("onboarding.routes.multizone.title")},null,8,["title"]),e(),l(O,null,{default:i(()=>[l(z,null,{header:i(()=>[l(Z,null,{title:i(()=>[e(`
              Add zones
            `)]),_:1})]),content:i(()=>[K,e(),n("p",R,[n("b",null,[e("See "),n("a",{href:`${m(f)("KUMA_DOCS_URL")}/deployments/multi-zone/?${m(f)("KUMA_UTM_QUERY_PARAMS")}#zone-control-plane`,target:"_blank"},"the documentation for options to install",8,D),e(".")])]),e(),n("div",null,[n("p",P,[e(`
              Zone status:

              `),s.value?(a(),c("span",G,"Connected")):(a(),c("span",j,"Disconnected"))]),e(),n("p",q,[e(`
              Zone ingress status:

              `),o.value?(a(),c("span",H,"Connected")):(a(),c("span",Q,"Disconnected"))]),e(),!o.value||!s.value?(a(),c("div",Y,[l(x)])):S("",!0)])]),navigation:i(()=>[l(I,{"next-step":"onboarding-create-mesh","previous-step":"onboarding-configuration-types","should-allow-next":s.value&&o.value},null,8,["should-allow-next"])]),_:1})]),_:1})]),_:1}))}});const oe=N(F,[["__scopeId","data-v-40900992"]]);export{oe as default};
