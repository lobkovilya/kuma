import{d as h,u as w,a as e,o as n,b as s,w as a,e as r,p as k,f as y,H as C,I as V,q as g}from"./index-d50afca2.js";import{_ as v}from"./ResourceCodeBlock.vue_vue_type_style_index_0_lang-6505800e.js";import"./CodeBlock.vue_vue_type_style_index_0_lang-2e43d83c.js";import"./toYaml-4e00099e.js";const $=h({__name:"DataPlaneConfigView",setup(S){const p=w();return(b,x)=>{const m=e("RouteTitle"),i=e("DataSource"),u=e("KCard"),_=e("AppView"),d=e("RouteView");return n(),s(d,{name:"data-plane-config-view",params:{mesh:"",dataPlane:"",codeSearch:""}},{default:a(({route:o,t:f})=>[r(_,null,{title:a(()=>[k("h2",null,[r(m,{title:f("data-planes.routes.item.navigation.data-plane-config-view")},null,8,["title"])])]),default:a(()=>[y(),r(u,null,{body:a(()=>[r(i,{src:`/meshes/${o.params.mesh}/dataplanes/${o.params.dataPlane}`},{default:a(({data:t,error:l})=>[l?(n(),s(C,{key:0,error:l},null,8,["error"])):t===void 0?(n(),s(V,{key:1})):(n(),s(v,{key:2,id:"code-block-data-plane",resource:t,"resource-fetcher":c=>g(p).getDataplaneFromMesh({mesh:t.mesh,name:t.name},c),"is-searchable":"",query:o.params.codeSearch,onQueryChange:c=>o.update({codeSearch:c})},null,8,["resource","resource-fetcher","query","onQueryChange"]))]),_:2},1032,["src"])]),_:2},1024)]),_:2},1024)]),_:1})}}});export{$ as default};