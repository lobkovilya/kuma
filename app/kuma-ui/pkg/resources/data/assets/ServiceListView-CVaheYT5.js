import{d as D,h as o,o as i,a as r,w as a,j as n,k as l,z as p,aB as A,C as R,X as g,t as c,b as _,F as d,S as B,M as L,r as N,e as h,A as P}from"./index-ChTNn-OB.js";import{e as T}from"./kong-icons.es249-CTaCOXIv.js";import{S as I}from"./SummaryView-B-JEVZY-.js";const K=D({__name:"ServiceListView",setup(X){return(q,E)=>{const y=o("RouteTitle"),u=o("XAction"),k=o("RouterView"),w=o("DataCollection"),C=o("DataLoader"),z=o("KCard"),S=o("AppView"),V=o("RouteView"),b=o("DataSource");return i(),r(b,{src:"/me"},{default:a(({data:v})=>[v?(i(),r(V,{key:0,name:"service-list-view",params:{page:1,size:v.pageSize,mesh:"",service:""}},{default:a(({route:s,t:m,uri:x})=>[n(y,{render:!1,title:m("services.routes.items.title")},null,8,["title"]),l(),n(S,null,{default:a(()=>[n(z,null,{default:a(()=>[n(C,{src:x(p(A),"/meshes/:mesh/service-insights/of/:serviceType",{mesh:s.params.mesh,serviceType:"internal"},{page:s.params.page,size:s.params.size})},{loadable:a(({data:t})=>[n(w,{type:"services",items:(t==null?void 0:t.items)??[void 0]},{default:a(()=>[n(R,{class:"service-collection","data-testid":"service-collection",headers:[{label:"Name",key:"name"},{label:"Address",key:"addressPort"},{label:"DP proxies (online / total)",key:"online"},{label:"Status",key:"status"},{label:"Details",key:"details",hideLabel:!0}],"page-number":s.params.page,"page-size":s.params.size,total:t==null?void 0:t.total,items:t==null?void 0:t.items,"is-selected-row":e=>e.name===s.params.service,onChange:s.update},{name:a(({row:e})=>[n(g,{text:e.name},{default:a(()=>[n(u,{to:{name:"service-detail-view",params:{mesh:e.mesh,service:e.name},query:{page:s.params.page,size:s.params.size}}},{default:a(()=>[l(c(e.name),1)]),_:2},1032,["to"])]),_:2},1032,["text"])]),addressPort:a(({row:e})=>[e.addressPort?(i(),r(g,{key:0,text:e.addressPort},null,8,["text"])):(i(),_(d,{key:1},[l(c(m("common.collection.none")),1)],64))]),online:a(({row:e})=>[e.dataplanes?(i(),_(d,{key:0},[l(c(e.dataplanes.online||0)+" / "+c(e.dataplanes.total||0),1)],64)):(i(),_(d,{key:1},[l(c(m("common.collection.none")),1)],64))]),status:a(({row:e})=>[n(B,{status:e.status},null,8,["status"])]),details:a(({row:e})=>[n(u,{class:"details-link","data-testid":"details-link",to:{name:"service-detail-view",params:{mesh:e.mesh,service:e.name}}},{default:a(()=>[l(c(m("common.collection.details_link"))+" ",1),n(p(T),{decorative:"",size:p(L)},null,8,["size"])]),_:2},1032,["to"])]),_:2},1032,["page-number","page-size","total","items","is-selected-row","onChange"]),l(),s.params.service?(i(),r(k,{key:0},{default:a(e=>[n(I,{onClose:f=>s.replace({name:"service-list-view",params:{mesh:s.params.mesh},query:{page:s.params.page,size:s.params.size}})},{default:a(()=>[(i(),r(N(e.Component),{name:s.params.service,service:t==null?void 0:t.items.find(f=>f.name===s.params.service)},null,8,["name","service"]))]),_:2},1032,["onClose"])]),_:2},1024)):h("",!0)]),_:2},1032,["items"])]),_:2},1032,["src"])]),_:2},1024)]),_:2},1024)]),_:2},1032,["params"])):h("",!0)]),_:1})}}}),M=P(K,[["__scopeId","data-v-7cec4de1"]]);export{M as default};
