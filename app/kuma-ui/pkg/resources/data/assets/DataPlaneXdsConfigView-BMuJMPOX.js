import{_ as l}from"./EnvoyData.vue_vue_type_script_setup_true_lang-r5H5poHM.js";import{d as c,a as o,o as m,b as _,w as t,e as n,m as u,f}from"./index-ANwvg_A1.js";import"./index-FZCiQto1.js";import"./CodeBlock-186e-woF.js";import"./uniqueId-8UY6iQFp.js";import"./ErrorBlock-K9GCAVpd.js";import"./TextWithCopyButton-Ac0tj8q8.js";import"./CopyButton-XxMKSpD7.js";import"./WarningIcon.vue_vue_type_script_setup_true_lang-lFA8wXpn.js";import"./LoadingBlock.vue_vue_type_script_setup_true_lang-1kzDND26.js";const B=c({__name:"DataPlaneXdsConfigView",setup(g){return(h,x)=>{const s=o("RouteTitle"),p=o("KCard"),r=o("AppView"),i=o("RouteView");return m(),_(i,{name:"data-plane-xds-config-view",params:{mesh:"",dataPlane:"",codeSearch:"",codeFilter:!1,codeRegExp:!1}},{default:t(({route:e,t:d})=>[n(r,null,{title:t(()=>[u("h2",null,[n(s,{title:d("data-planes.routes.item.navigation.data-plane-xds-config-view")},null,8,["title"])])]),default:t(()=>[f(),n(p,null,{default:t(()=>[n(l,{resource:"Data Plane Proxy",src:`/meshes/${e.params.mesh}/dataplanes/${e.params.dataPlane}/data-path/xds`,query:e.params.codeSearch,"is-filter-mode":e.params.codeFilter,"is-reg-exp-mode":e.params.codeRegExp,onQueryChange:a=>e.update({codeSearch:a}),onFilterModeChange:a=>e.update({codeFilter:a}),onRegExpModeChange:a=>e.update({codeRegExp:a})},null,8,["src","query","is-filter-mode","is-reg-exp-mode","onQueryChange","onFilterModeChange","onRegExpModeChange"])]),_:2},1024)]),_:2},1024)]),_:1})}}});export{B as default};