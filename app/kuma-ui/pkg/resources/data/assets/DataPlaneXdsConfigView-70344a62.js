import{E as l}from"./EnvoyData-342ba973.js";import{d as m,a as o,o as c,b as _,w as t,e as n,m as u,f as g}from"./index-5d5446a4.js";import"./index-fce48c05.js";import"./CodeBlock.vue_vue_type_style_index_0_lang-be5bb8ae.js";import"./uniqueId-90cc9b93.js";import"./EmptyBlock.vue_vue_type_script_setup_true_lang-710db794.js";import"./ErrorBlock-567378ca.js";import"./TextWithCopyButton-1669005d.js";import"./CopyButton-b62a1694.js";import"./WarningIcon.vue_vue_type_script_setup_true_lang-e798630e.js";import"./LoadingBlock.vue_vue_type_script_setup_true_lang-52bdda9b.js";const D=m({__name:"DataPlaneXdsConfigView",setup(f){return(h,x)=>{const p=o("RouteTitle"),r=o("KCard"),s=o("AppView"),i=o("RouteView");return c(),_(i,{name:"data-plane-xds-config-view",params:{mesh:"",dataPlane:"",codeSearch:"",codeFilter:!1,codeRegExp:!1}},{default:t(({route:e,t:d})=>[n(s,null,{title:t(()=>[u("h2",null,[n(p,{title:d("data-planes.routes.item.navigation.data-plane-xds-config-view")},null,8,["title"])])]),default:t(()=>[g(),n(r,null,{default:t(()=>[n(l,{resource:"Data Plane Proxy",src:`/meshes/${e.params.mesh}/dataplanes/${e.params.dataPlane}/data-path/xds`,query:e.params.codeSearch,"is-filter-mode":e.params.codeFilter,"is-reg-exp-mode":e.params.codeRegExp,onQueryChange:a=>e.update({codeSearch:a}),onFilterModeChange:a=>e.update({codeFilter:a}),onRegExpModeChange:a=>e.update({codeRegExp:a})},null,8,["src","query","is-filter-mode","is-reg-exp-mode","onQueryChange","onFilterModeChange","onRegExpModeChange"])]),_:2},1024)]),_:2},1024)]),_:1})}}});export{D as default};