import{d as h,a as d,o as c,b as y,w as t,e as o,V as b,f as e,m as i,X as r,t as a,c as p,q as k,F as V,D as v}from"./index-C5Gyl9xU.js";import{S as w}from"./StatusBadge-BhivBoqR.js";import{_ as g}from"./SubscriptionList.vue_vue_type_script_setup_true_lang-BAdqjcUD.js";import"./AccordionList-B1opKuAT.js";const z=["data-testid","innerHTML"],C={"data-testid":"detail-view-details",class:"stack"},B={class:"columns"},x={key:0},L=h({__name:"ZoneDetailView",props:{data:{},notifications:{default:()=>[]}},setup(m){const s=m;return(D,I)=>{const u=d("KCard"),_=d("AppView"),f=d("RouteView");return c(),y(f,{name:"zone-cp-detail-view"},{default:t(({t:n})=>[o(_,null,b({default:t(()=>[e(),i("div",C,[o(u,null,{default:t(()=>[i("div",B,[o(r,null,{title:t(()=>[e(a(n("http.api.property.status")),1)]),body:t(()=>[o(w,{status:s.data.state},null,8,["status"])]),_:2},1024),e(),o(r,null,{title:t(()=>[e(a(n("http.api.property.type")),1)]),body:t(()=>[e(a(n(`common.product.environment.${s.data.zoneInsight.environment||"unknown"}`)),1)]),_:2},1024),e(),o(r,null,{title:t(()=>[e(a(n("zone-cps.routes.item.authentication_type")),1)]),body:t(()=>[e(a(s.data.zoneInsight.authenticationType||n("common.not_applicable")),1)]),_:2},1024)])]),_:2},1024),e(),s.data.zoneInsight.subscriptions.length>0?(c(),p("div",x,[i("h2",null,a(n("zone-cps.detail.subscriptions")),1),e(),o(u,{class:"mt-4"},{default:t(()=>[o(g,{subscriptions:s.data.zoneInsight.subscriptions},{default:t(()=>[i("p",null,a(n("zone-cps.routes.item.subscription_intro")),1)]),_:2},1032,["subscriptions"])]),_:2},1024)])):k("",!0)])]),_:2},[s.notifications.length>0?{name:"notifications",fn:t(()=>[i("ul",null,[(c(!0),p(V,null,v(s.notifications,l=>(c(),p("li",{key:l.kind,"data-testid":`warning-${l.kind}`,innerHTML:n(`common.warnings.${l.kind}`,l.payload)},null,8,z))),128))])]),key:"0"}:void 0]),1024)]),_:1})}}});export{L as default};