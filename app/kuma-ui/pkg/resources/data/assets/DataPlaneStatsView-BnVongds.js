import{d as C,h as t,o as w,a as x,w as o,j as a,k as s,z as r,M as R}from"./index-ChTNn-OB.js";import{f as k}from"./kong-icons.es350-PMU6AsPW.js";import{C as V}from"./CodeBlock-DcRwY4Tw.js";const M=C({__name:"DataPlaneStatsView",props:{data:{}},setup(d){const p=d;return(y,E)=>{const c=t("RouteTitle"),l=t("KButton"),i=t("DataLoader"),m=t("KCard"),_=t("AppView"),u=t("RouteView");return w(),x(u,{name:"data-plane-stats-view",params:{mesh:"",dataPlane:"",codeSearch:"",codeFilter:!1,codeRegExp:!1}},{default:o(({route:e,t:f})=>[a(c,{render:!1,title:f("data-planes.routes.item.navigation.data-plane-stats-view")},null,8,["title"]),s(),a(_,null,{default:o(()=>[a(m,null,{default:o(()=>[a(i,{src:`/meshes/${e.params.mesh}/dataplanes/${e.params.dataPlane}/stats/${p.data.dataplane.networking.inboundAddress}`},{default:o(({data:g,refresh:h})=>[a(V,{language:"json",code:g.raw,"is-searchable":"",query:e.params.codeSearch,"is-filter-mode":e.params.codeFilter,"is-reg-exp-mode":e.params.codeRegExp,onQueryChange:n=>e.update({codeSearch:n}),onFilterModeChange:n=>e.update({codeFilter:n}),onRegExpModeChange:n=>e.update({codeRegExp:n})},{"primary-actions":o(()=>[a(l,{appearance:"primary",onClick:h},{default:o(()=>[a(r(k),{size:r(R)},null,8,["size"]),s(`
                Refresh
              `)]),_:2},1032,["onClick"])]),_:2},1032,["code","query","is-filter-mode","is-reg-exp-mode","onQueryChange","onFilterModeChange","onRegExpModeChange"])]),_:2},1032,["src"])]),_:2},1024)]),_:2},1024)]),_:1})}}});export{M as default};
