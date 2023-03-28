import{_ as P}from"./PolicyConnections.vue_vue_type_script_setup_true_lang-94808375.js";import{_ as x}from"./EmptyBlock.vue_vue_type_script_setup_true_lang-467e5ff3.js";import{E as B}from"./ErrorBlock-e347ad5a.js";import{_ as E}from"./LabelList.vue_vue_type_style_index_0_lang-4dec2f30.js";import{_ as N}from"./LoadingBlock.vue_vue_type_script_setup_true_lang-7adf9901.js";import{T as V}from"./TabsWidget-760376e0.js";import{_ as $}from"./YamlView.vue_vue_type_script_setup_true_lang-8340ad04.js";import{d as D,l as T,r as u,o as a,c as s,k as n,x as m,e as p,w as r,g as l,y as d,a as f,F as h,z as A,u as C,J as F}from"./index-e096fb01.js";import"./QueryParameter-70743f73.js";import"./CodeBlock.vue_vue_type_style_index_0_lang-25903ab0.js";import"./toYaml-4e00099e.js";const L={class:"policy-details component-frame"},S={class:"entity-heading","data-testid":"policy-single-entity"},z={"data-testid":"policy-overview-tab"},H={key:0},U=D({__name:"PolicyDetailView",props:{mesh:null,policyPath:null,policyName:null},setup(k){const _=k,g=T(),w=[{hash:"#overview",title:"Overview"},{hash:"#affected-dpps",title:"Affected DPPs"}],e=u(null),i=u(!0),o=u(null);async function b({mesh:y,policyPath:v,policyName:c}){i.value=!0,o.value=null,e.value=null;try{e.value=await g.getSinglePolicyEntity({mesh:y,path:v,name:c})}catch(t){t instanceof Error?o.value=t:console.error(t)}finally{i.value=!1}}return b(_),(y,v)=>(a(),s("div",L,[i.value?(a(),n(N,{key:0})):o.value!==null?(a(),n(B,{key:1,error:o.value},null,8,["error"])):e.value===null?(a(),n(x,{key:2})):m("",!0),p(),e.value!==null?(a(),n(V,{key:3,tabs:w},{tabHeader:r(()=>[l("h1",S,d(e.value.name),1)]),overview:r(()=>[f(E,null,{default:r(()=>[l("div",z,[l("ul",null,[(a(!0),s(h,null,A(e.value,(c,t)=>(a(),s(h,{key:t},[["type","mesh","name"].includes(t)?(a(),s("li",H,[l("h4",null,d(t),1),p(),l("p",null,d(c),1)])):m("",!0)],64))),128))])])]),_:1}),p(),f($,{id:"code-block-policy",class:"mt-4",content:C(F)(e.value),"is-searchable":""},null,8,["content"])]),"affected-dpps":r(()=>[f(P,{mesh:e.value.mesh,"policy-name":e.value.name,"policy-type":_.policyPath},null,8,["mesh","policy-name","policy-type"])]),_:1})):m("",!0)]))}});export{U as default};
