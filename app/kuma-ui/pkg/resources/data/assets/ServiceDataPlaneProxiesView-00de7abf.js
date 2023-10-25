import{D as V,K as k}from"./KFilterBar-338a9927.js";import{d as C,r as o,o as i,i as l,w as a,j as s,p as z,n as p,l as q,F as P,I as T,H as B,m as y,t as K}from"./index-f09cca58.js";import"./AppCollection-4b4f9dc8.js";import"./EmptyBlock.vue_vue_type_script_setup_true_lang-bb9bf655.js";import"./StatusBadge-3b00ac53.js";import"./dataplane-0a086c06.js";const $=C({__name:"ServiceDataPlaneProxiesView",setup(F){return(N,I)=>{const f=o("RouteTitle"),v=o("KSelect"),b=o("KCard"),c=o("DataSource"),h=o("AppView"),w=o("RouteView");return i(),l(c,{src:"/me"},{default:a(({data:m})=>[m?(i(),l(w,{key:0,name:"service-data-plane-proxies-view",params:{page:1,size:m.pageSize,query:"",s:"",mesh:"",service:"",gatewayType:""}},{default:a(({route:e,t:S})=>[s(h,null,{title:a(()=>[z("h2",null,[s(f,{title:S("services.routes.item.navigation.service-data-plane-proxies-view"),render:!0},null,8,["title"])])]),default:a(()=>[p(),s(c,{src:`/meshes/${e.params.mesh}/dataplanes/for/${e.params.service}/of/all?page=${e.params.page}&size=${e.params.size}&search=${e.params.s}`},{default:a(({data:r,error:x})=>{var u,_,d,g;return[(i(!0),q(P,null,T([((g=(d=(_=(u=r==null?void 0:r.items)==null?void 0:u[0])==null?void 0:_.dataplane)==null?void 0:d.networking)==null?void 0:g.gateway)!==void 0],n=>(i(),l(b,{key:n},{body:a(()=>[s(V,{"data-testid":"data-plane-collection",class:"data-plane-collection","page-number":parseInt(e.params.page),"page-size":parseInt(e.params.size),total:r==null?void 0:r.total,items:r==null?void 0:r.items,error:x,gateways:n,onChange:e.update},{toolbar:a(()=>[s(k,{class:"data-plane-proxy-filter",placeholder:"tag: 'kuma.io/protocol: http'",query:e.params.query,fields:{name:{description:"filter by name or parts of a name"},protocol:{description:"filter by “kuma.io/protocol” value"},tag:{description:"filter by tags (e.g. “tag: version:2”)"},zone:{description:"filter by “kuma.io/zone” value"}},onFieldsChange:t=>e.update({query:t.query,s:t.query.length>0?JSON.stringify(t.fields):""})},null,8,["placeholder","query","fields","onFieldsChange"]),p(),n?(i(),l(v,{key:0,label:"Type","overlay-label":!0,items:[{label:"All",value:"all"},{label:"Builtin",value:"builtin"},{label:"Delegated",value:"delegated"}].map(t=>({...t,selected:t.value===e.params.gatewayType})),appearance:"select",onSelected:t=>e.update({gatewayType:String(t.value)})},{"item-template":a(({item:t})=>[p(B(t.label),1)]),_:2},1032,["items","onSelected"])):y("",!0)]),_:2},1032,["page-number","page-size","total","items","error","gateways","onChange"])]),_:2},1024))),128))]}),_:2},1032,["src"])]),_:2},1024)]),_:2},1032,["params"])):y("",!0)]),_:1})}}});const J=K($,[["__scopeId","data-v-428bf3f3"]]);export{J as default};