import{E as i}from"./EnvoyData-c65fec9b.js";import{a as m}from"./dataplane-0a086c06.js";import{d as _,r as e,o as h,i as f,w as t,j as s,p as w,n as C,k as V}from"./index-a6f5023f.js";import"./CodeBlock.vue_vue_type_style_index_0_lang-ad731d3d.js";import"./EmptyBlock.vue_vue_type_script_setup_true_lang-1974ccfb.js";const k=_({__name:"DataPlaneClustersView",props:{data:{}},setup(o){const n=o;return(y,g)=>{const r=e("RouteTitle"),l=e("KCard"),p=e("AppView"),c=e("RouteView");return h(),f(c,{name:"data-plane-clusters-view",params:{mesh:"",dataPlane:"",codeSearch:""}},{default:t(({route:a,t:d})=>[s(p,null,{title:t(()=>[w("h2",null,[s(r,{title:d("data-planes.routes.item.navigation.data-plane-clusters-view"),render:!0},null,8,["title"])])]),default:t(()=>[C(),s(l,null,{body:t(()=>[s(i,{status:V(m)(n.data.dataplane,n.data.dataplaneInsight).status,resource:"Data Plane Proxy",src:`/meshes/${a.params.mesh}/dataplanes/${a.params.dataPlane}/data-path/clusters`,query:a.params.codeSearch,onQueryChange:u=>a.update({codeSearch:u})},null,8,["status","src","query","onQueryChange"])]),_:2},1024)]),_:2},1024)]),_:1})}}});export{k as default};