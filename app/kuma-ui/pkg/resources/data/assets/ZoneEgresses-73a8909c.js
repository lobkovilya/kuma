import{d as $,l as M,m as Q,r as o,n as R,p as U,s as G,t as H,o as u,c as d,a as l,w as a,u as N,P as q,k as _,e as E,E as K,x as b,g,y as z,F as B,z as I,B as W,A as X}from"./index-e096fb01.js";import{D as Y}from"./DataOverview-8ec2eb8a.js";import{E as w}from"./EnvoyData-e00d5aaf.js";import{F as j}from"./FrameSkeleton-f310b9e4.js";import{_ as J}from"./LabelList.vue_vue_type_style_index_0_lang-4dec2f30.js";import{_ as ee,S as ae}from"./SubscriptionHeader.vue_vue_type_script_setup_true_lang-bf69e6e6.js";import{T as se}from"./TabsWidget-760376e0.js";import{Q as O}from"./QueryParameter-70743f73.js";import"./EmptyBlock.vue_vue_type_script_setup_true_lang-467e5ff3.js";import"./ErrorBlock-e347ad5a.js";import"./LoadingBlock.vue_vue_type_script_setup_true_lang-7adf9901.js";import"./TagList-91f2b0bd.js";import"./StatusBadge-79f7109b.js";import"./CodeBlock.vue_vue_type_style_index_0_lang-25903ab0.js";const te={class:"zoneegresses"},ne={class:"entity-heading"},re={key:0},Ee=$({__name:"ZoneEgresses",props:{selectedZoneEgressName:{type:String,required:!1,default:null},offset:{type:Number,required:!1,default:0}},setup(C){const p=C,k=M(),F={title:"No Data",message:"There are no Zone Egresses present."},L=[{hash:"#overview",title:"Overview"},{hash:"#insights",title:"Zone Egress Insights"},{hash:"#xds-configuration",title:"XDS Configuration"},{hash:"#envoy-stats",title:"Stats"},{hash:"#envoy-clusters",title:"Clusters"}],f=Q(),m=o(!0),c=o(!1),v=o(null),y=o({headers:[{label:"Status",key:"status"},{label:"Name",key:"name"}],data:[]}),i=o(null),x=o([]),S=o(null),A=o([]),D=o(p.offset);R(()=>f.params.mesh,function(){f.name==="zoneegresses"&&(m.value=!0,c.value=!1,v.value=null,h(0))}),U(function(){h(p.offset)});async function h(s){D.value=s,O.set("offset",s>0?s:null),m.value=!0,c.value=!1;const t=f.query.ns||null,r=q;try{const{data:e,next:n}=await V(t,r,s);S.value=n,e.length?(c.value=!1,x.value=e,T({name:p.selectedZoneEgressName??e[0].name}),y.value.data=e.map(Z=>{const P=G(Z.zoneEgressInsight??{});return{...Z,status:P}})):(y.value.data=[],c.value=!0)}catch(e){e instanceof Error?v.value=e:console.error(e),c.value=!0}finally{m.value=!1}}function T({name:s}){var e;const t=x.value.find(n=>n.name===s),r=((e=t==null?void 0:t.zoneEgressInsight)==null?void 0:e.subscriptions)??[];A.value=Array.from(r).reverse(),i.value=H(t,["type","name"]),O.set("zoneEgress",s)}async function V(s,t,r){if(s)return{data:[await k.getZoneEgressOverview({name:s},{size:t,offset:r})],next:null};{const{items:e,next:n}=await k.getAllZoneEgressOverviews({size:t,offset:r});return{data:e??[],next:n}}}return(s,t)=>(u(),d("div",te,[l(j,null,{default:a(()=>{var r;return[l(Y,{"selected-entity-name":(r=i.value)==null?void 0:r.name,"page-size":N(q),"is-loading":m.value,error:v.value,"empty-state":F,"table-data":y.value,"table-data-is-empty":c.value,next:S.value,"page-offset":D.value,onTableAction:T,onLoadData:h},{additionalControls:a(()=>[s.$route.query.ns?(u(),_(N(K),{key:0,class:"back-button",appearance:"primary",icon:"arrowLeft",to:{name:"zoneegresses"}},{default:a(()=>[E(`
            View all
          `)]),_:1})):b("",!0)]),_:1},8,["selected-entity-name","page-size","is-loading","error","table-data","table-data-is-empty","next","page-offset"]),E(),c.value===!1&&i.value!==null?(u(),_(se,{key:0,"has-error":v.value!==null,"is-loading":m.value,tabs:L},{tabHeader:a(()=>[g("h1",ne,`
            Zone Egress: `+z(i.value.name),1)]),overview:a(()=>[l(J,null,{default:a(()=>[g("div",null,[g("ul",null,[(u(!0),d(B,null,I(i.value,(e,n)=>(u(),d("li",{key:n},[e?(u(),d("h4",re,z(n),1)):b("",!0),E(),g("p",null,z(e),1)]))),128))])])]),_:1})]),insights:a(()=>[l(W,{"initially-open":0},{default:a(()=>[(u(!0),d(B,null,I(A.value,(e,n)=>(u(),_(X,{key:n},{"accordion-header":a(()=>[l(ee,{details:e},null,8,["details"])]),"accordion-content":a(()=>[l(ae,{details:e,"is-discovery-subscription":""},null,8,["details"])]),_:2},1024))),128))]),_:1})]),"xds-configuration":a(()=>[l(w,{"data-path":"xds","zone-egress-name":i.value.name,"query-key":"envoy-data-zone-egress"},null,8,["zone-egress-name"])]),"envoy-stats":a(()=>[l(w,{"data-path":"stats","zone-egress-name":i.value.name,"query-key":"envoy-data-zone-egress"},null,8,["zone-egress-name"])]),"envoy-clusters":a(()=>[l(w,{"data-path":"clusters","zone-egress-name":i.value.name,"query-key":"envoy-data-zone-egress"},null,8,["zone-egress-name"])]),_:1},8,["has-error","is-loading"])):b("",!0)]}),_:1})]))}});export{Ee as default};
