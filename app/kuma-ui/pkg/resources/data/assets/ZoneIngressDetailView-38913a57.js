import{d as w,u as k,q as l,s as _,o as s,a as o,w as f,h as m,b as p,g as z,k as b,e as h}from"./index-e8e69e62.js";import{_ as y}from"./ZoneIngressDetails.vue_vue_type_script_setup_true_lang-695642f8.js";import{j as I,k as $,g as x,_ as B}from"./RouteView.vue_vue_type_script_setup_true_lang-b28dd8e7.js";import{_ as E}from"./RouteTitle.vue_vue_type_script_setup_true_lang-8f9a216a.js";import{_ as V}from"./EmptyBlock.vue_vue_type_script_setup_true_lang-04152738.js";import{E as N}from"./ErrorBlock-63a54e88.js";import{_ as A}from"./LoadingBlock.vue_vue_type_script_setup_true_lang-63e74df2.js";import"./DefinitionListItem-86d63286.js";import"./EnvoyData-ba2efeda.js";import"./kongponents.es-aa96ab2e.js";import"./CodeBlock.vue_vue_type_style_index_0_lang-cc5f6631.js";import"./StatusInfo.vue_vue_type_script_setup_true_lang-a5fbb478.js";import"./SubscriptionHeader.vue_vue_type_script_setup_true_lang-f4494a0b.js";import"./TabsWidget-5e831f58.js";import"./QueryParameter-70743f73.js";import"./TextWithCopyButton-73715a8a.js";const C={class:"zone-details"},D={key:3,class:"kcard-border","data-testid":"detail-view-details"},W=w({__name:"ZoneIngressDetailView",setup(O){const d=I(),e=k(),{t:u}=$(),a=l(null),n=l(!0),r=l(null);_(()=>e.params.mesh,function(){e.name==="zone-ingress-detail-view"&&i()}),_(()=>e.params.name,function(){e.name==="zone-ingress-detail-view"&&i()}),v();function v(){i()}async function i(){n.value=!0,r.value=null;const c=e.params.zoneIngress;try{a.value=await d.getZoneIngressOverview({name:c})}catch(t){a.value=null,t instanceof Error?r.value=t:console.error(t)}finally{n.value=!1}}return(c,t)=>(s(),o(B,null,{default:f(({route:g})=>[m(E,{title:p(u)("zone-ingresses.routes.item.title",{name:g.params.zoneIngress})},null,8,["title"]),z(),m(x,{breadcrumbs:[{to:{name:"zone-ingress-list-view"},text:p(u)("zone-ingresses.routes.item.breadcrumbs")}]},{default:f(()=>[b("div",C,[n.value?(s(),o(A,{key:0})):r.value!==null?(s(),o(N,{key:1,error:r.value},null,8,["error"])):a.value===null?(s(),o(V,{key:2})):(s(),h("div",D,[m(y,{"zone-ingress-overview":a.value},null,8,["zone-ingress-overview"])]))])]),_:1},8,["breadcrumbs"])]),_:1}))}});export{W as default};