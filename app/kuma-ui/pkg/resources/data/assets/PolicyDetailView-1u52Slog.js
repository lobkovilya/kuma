import{d as D,h as t,o as l,a as p,w as a,j as n,z as r,aA as v,C as b,k as i,t as m,b as P,F as S,M as N,r as R,e as _,A as x}from"./index-ChTNn-OB.js";import{e as A}from"./kong-icons.es249-CTaCOXIv.js";import{S as L}from"./SummaryView-B-JEVZY-.js";const I=D({__name:"PolicyDetailView",setup(B){return(K,E)=>{const c=t("RouterLink"),y=t("DataCollection"),f=t("RouterView"),h=t("DataLoader"),z=t("KCard"),k=t("AppView"),w=t("RouteView"),C=t("DataSource");return l(),p(C,{src:"/me"},{default:a(({data:d})=>[d?(l(),p(w,{key:0,name:"policy-detail-view",params:{mesh:"",policy:"",policyPath:"",dataPlane:"",s:"",page:1,size:d.pageSize}},{default:a(({route:e,t:u,uri:g,can:V})=>[n(k,null,{default:a(()=>[n(z,null,{default:a(()=>[n(h,{src:g(r(v),"/meshes/:mesh/policy-path/:path/policy/:name/dataplanes",{mesh:e.params.mesh,path:e.params.policyPath,name:e.params.policy},{page:e.params.page,size:e.params.size})},{loadable:a(({data:o})=>[n(y,{type:"data-planes",items:(o==null?void 0:o.items)??[void 0]},{default:a(()=>[n(b,{"page-number":e.params.page,"page-size":e.params.size,headers:[{label:"Name",key:"name"},{label:"Namespace",key:"namespace"},...V("use zones")?[{label:"Zone",key:"zone"}]:[],{label:"Details",key:"details",hideLabel:!0}],items:o==null?void 0:o.items,total:o==null?void 0:o.total,"is-selected-row":s=>s.id===e.params.dataPlane,onChange:e.update},{name:a(({row:s})=>[n(c,{"data-action":"",to:{name:"data-plane-detail-view",params:{dataPlane:s.id}}},{default:a(()=>[i(m(s.name),1)]),_:2},1032,["to"])]),namespace:a(({row:s})=>[i(m(s.namespace),1)]),zone:a(({row:s})=>[s.zone?(l(),p(c,{key:0,to:{name:"zone-cp-detail-view",params:{zone:s.zone}}},{default:a(()=>[i(m(s.zone),1)]),_:2},1032,["to"])):(l(),P(S,{key:1},[i(m(u("common.collection.none")),1)],64))]),details:a(({row:s})=>[n(c,{class:"details-link","data-testid":"details-link",to:{name:"data-plane-detail-view",params:{dataPlane:s.id}}},{default:a(()=>[i(m(u("common.collection.details_link"))+" ",1),n(r(A),{decorative:"",size:r(N)},null,8,["size"])]),_:2},1032,["to"])]),_:2},1032,["page-number","page-size","headers","items","total","is-selected-row","onChange"])]),_:2},1032,["items"]),i(),n(f,null,{default:a(({Component:s})=>[e.child()?(l(),p(L,{key:0,onClose:F=>e.replace({params:{mesh:e.params.mesh},query:{page:e.params.page,size:e.params.size,s:e.params.s}})},{default:a(()=>[typeof o<"u"?(l(),p(R(s),{key:0,items:o.items},null,8,["items"])):_("",!0)]),_:2},1032,["onClose"])):_("",!0)]),_:2},1024)]),_:2},1032,["src"])]),_:2},1024)]),_:2},1024)]),_:2},1032,["params"])):_("",!0)]),_:1})}}}),M=x(I,[["__scopeId","data-v-e08cfcfd"]]);export{M as default};
