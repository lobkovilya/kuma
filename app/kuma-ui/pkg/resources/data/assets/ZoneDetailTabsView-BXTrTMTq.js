import{d as O,U as Z,l as B,D as L,a as t,o as _,b as u,w as e,e as n,p as a,Q as w,m as f,T as S,f as i,G as K,t as d,E as M,a3 as $,q as z,a1 as C}from"./index-Dlb3STpa.js";import{_ as G}from"./DeleteResourceModal.vue_vue_type_script_setup_true_lang-D1OwWJon.js";const Q=O({__name:"ZoneDetailTabsView",setup(P){const x=Z(),{t:o}=B(),b=L([]),h=c=>{const r=[];c.zoneInsight.store==="memory"&&r.push({kind:"ZONE_STORE_TYPE_MEMORY",payload:{}}),C(c.zoneInsight,"version.kumaCp.kumaCpGlobalCompatible","true")||r.push({kind:"INCOMPATIBLE_ZONE_AND_GLOBAL_CPS_VERSIONS",payload:{zoneCpVersion:C(c.zoneInsight,"version.kumaCp.version",o("common.collection.none"))}}),b.value=r};async function v(c){await x.deleteZone({name:c})}return(c,r)=>{const D=t("RouteTitle"),T=t("KDropdownItem"),g=t("XTeleportTemplate"),k=t("XDisclosure"),y=t("KDropdown"),V=t("XAction"),A=t("XTabs"),E=t("RouterView"),I=t("AppView"),N=t("DataLoader"),R=t("RouteView");return _(),u(R,{name:"zone-cp-detail-tabs-view",params:{zone:""}},{default:e(({can:X,route:l})=>[n(N,{src:`/zone-cps/${l.params.zone}`,onChange:h},{default:e(({data:m})=>[m?(_(),u(I,{key:0,breadcrumbs:[{to:{name:"zone-cp-list-view"},text:a(o)("zone-cps.routes.item.breadcrumbs")}]},w({title:e(()=>[f("h1",null,[n(S,{text:l.params.zone},{default:e(()=>[n(D,{title:a(o)("zone-cps.routes.item.title",{name:l.params.zone})},null,8,["title"])]),_:2},1032,["text"])])]),default:e(()=>{var p;return[i(),i(),n(A,{selected:(p=l.active)==null?void 0:p.name},w({_:2},[K(l.children,({name:s})=>({name:`${s}-tab`,fn:e(()=>[n(V,{to:{name:s}},{default:e(()=>[i(d(a(o)(`zone-cps.routes.item.navigation.${s}`)),1)]),_:2},1032,["to"])])}))]),1032,["selected"]),i(),n(E,null,{default:e(s=>[(_(),u(M(s.Component),{data:m,notifications:b.value},null,8,["data","notifications"]))]),_:2},1024)]}),_:2},[X("create zones")?{name:"actions",fn:e(()=>[n(y,{"kpop-attributes":{placement:"bottomEnd"},"trigger-text":a(o)("zones.action_menu.toggle_button"),"show-caret":"",width:"280"},{items:e(()=>[n(k,null,{default:e(({expanded:p,toggle:s})=>[n(T,{danger:"","data-testid":"delete-button",onClick:$(s,["prevent"])},{default:e(()=>[i(d(a(o)("zones.action_menu.delete_button")),1)]),_:2},1032,["onClick"]),i(),n(g,{to:{name:"modal-layer"}},{default:e(()=>[p?(_(),u(G,{key:0,"confirmation-text":m.name,"delete-function":()=>v(m.name),"is-visible":"","action-button-text":a(o)("common.delete_modal.proceed_button"),title:a(o)("common.delete_modal.title",{type:"Zone"}),"data-testid":"delete-zone-modal",onCancel:s,onDelete:()=>l.replace({name:"zone-cp-list-view"})},{default:e(()=>[f("p",null,d(a(o)("common.delete_modal.text1",{type:"Zone",name:m.name})),1),i(),f("p",null,d(a(o)("common.delete_modal.text2")),1)]),_:2},1032,["confirmation-text","delete-function","action-button-text","title","onCancel","onDelete"])):z("",!0)]),_:2},1024)]),_:2},1024)]),_:2},1032,["trigger-text"])]),key:"0"}:void 0]),1032,["breadcrumbs"])):z("",!0)]),_:2},1032,["src"])]),_:1})}}});export{Q as default};