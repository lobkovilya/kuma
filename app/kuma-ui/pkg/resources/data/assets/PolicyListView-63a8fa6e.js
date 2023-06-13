import{l as Z,T as F,Z as W,V as j,v as G}from"./kongponents.es-208ec824.js";import{d as I,o as _,a as P,b as l,i as r,e as t,h as c,u as H,D as Y,s as d,c as g,w as J,f as X,l as y,g as N,P as V,H as L,t as ee,p as ae,n as te}from"./index-6fad8e68.js";import{_ as se}from"./PolicyDetails.vue_vue_type_script_setup_true_lang-f9dc0516.js";import{l as le,j as oe,f as ne,k as ie,g as re,_ as ce,h as pe}from"./RouteView.vue_vue_type_script_setup_true_lang-d7cfbc23.js";import{_ as ue}from"./RouteTitle.vue_vue_type_script_setup_true_lang-d610bc33.js";import{D as me}from"./DataOverview-e0533261.js";import{Q as $}from"./QueryParameter-70743f73.js";import"./StatusInfo.vue_vue_type_script_setup_true_lang-20c1797b.js";import"./EmptyBlock.vue_vue_type_script_setup_true_lang-3b6f87d5.js";import"./ErrorBlock-bdf8b3c1.js";import"./LoadingBlock.vue_vue_type_script_setup_true_lang-5faa885b.js";import"./ResourceCodeBlock.vue_vue_type_script_setup_true_lang-cdef82c3.js";import"./CodeBlock.vue_vue_type_style_index_0_lang-56641179.js";import"./TextWithCopyButton-478fead2.js";import"./toYaml-4e00099e.js";import"./TabsWidget-13cdf8bf.js";import"./TagList-73b44c1f.js";import"./StatusBadge-6ed545a8.js";const de=I({__name:"DocumentationLink",props:{href:{type:String,required:!0}},setup(o){const a=o;return(k,D)=>(_(),P(t(F),{class:"docs-link",appearance:"outline",target:"_blank",to:a.href},{default:l(()=>[r(t(Z),{icon:"externalLink",color:"currentColor",size:"16","hide-title":""}),c(`

    Documentation
  `)]),_:1},8,["to"]))}}),ye=o=>(ae("data-v-32be22cb"),o=o(),te(),o),he={class:"kcard-stack"},fe={class:"kcard-border"},_e=ye(()=>y("p",null,[y("strong",null,"Warning"),c(` This policy is experimental. If you encountered any problem please open an
                      `),y("a",{href:"https://github.com/kumahq/kuma/issues/new/choose",target:"_blank",rel:"noopener noreferrer"},"issue")],-1)),ve=I({__name:"PolicyListView",props:{selectedPolicyName:{type:[String,null],required:!1,default:null},policyPath:{type:String,required:!0},offset:{type:Number,required:!1,default:0}},setup(o){const a=o,k=le(),D=oe(),n=H(),B=Y(),i=ne(),{t:C}=ie(),T=d(!0),w=d(null),S=d(null),A=d(a.offset),v=d(a.selectedPolicyName),h=d({headers:[{label:"Name",key:"entity"},{label:"Type",key:"type"}],data:[]}),M=g(()=>n.params.mesh),s=g(()=>i.state.policyTypesByPath[a.policyPath]),U=g(()=>i.state.policyTypes.map(e=>({label:e.name,value:e.path,selected:e.path===a.policyPath}))),R=g(()=>i.state.policyTypes.filter(e=>(i.state.sidebar.insights.mesh.policies[e.name]??0)===0).map(e=>e.name));J(()=>n.params.mesh,function(){n.name===a.policyPath&&x(0)}),q();async function q(){const e=i.state.policyTypesByPath[a.policyPath];e!==void 0&&(await i.dispatch("updatePageTitle",""),await i.dispatch("updatePageTitle",e.name)),x(a.offset)}async function x(e){var b;A.value=e,$.set("offset",e>0?e:null),T.value=!0,w.value=null;const p=n.params.mesh,u=n.params.policyPath,m=V;try{const{items:f,next:Q}=await D.getAllPolicyEntitiesFromMesh({mesh:p,path:u},{size:m,offset:e});S.value=Q,h.value.data=z(f??[]),E({name:a.selectedPolicyName??((b=h.value.data[0])==null?void 0:b.entity.name)})}catch(f){h.value.data=[],f instanceof Error?w.value=f:console.error(f)}finally{T.value=!1}}function z(e){return e.map(p=>{const{type:u,name:m}=p,b={name:"policy-detail-view",params:{mesh:p.mesh,policyPath:n.params.policyPath,policy:m}};return{entity:p,detailViewRoute:b,type:u}})}function K(e){E({name:e.name})}function E({name:e}){v.value=e??null,$.set("policy",e??null)}function O(e){B.push({name:"policies-list-view",params:{...n.params,policyPath:e.value}})}return(e,p)=>(_(),P(ce,{module:"policies"},{default:l(()=>{var u;return[r(ue,{title:t(C)("policies.routes.items.title",{name:(u=s.value)==null?void 0:u.name})},null,8,["title"]),c(),r(re,null,{default:l(()=>[s.value?(_(),X("div",{key:0,class:L(["relative",s.value.path])},[y("div",he,[y("div",fe,[s.value.isExperimental?(_(),P(t(W),{key:0,"border-variant":"noBorder",class:"mb-4"},{body:l(()=>[r(t(j),{appearance:"warning"},{alertMessage:l(()=>[_e]),_:1})]),_:1})):N("",!0),c(),r(me,{"selected-entity-name":v.value??void 0,"page-size":t(V),error:w.value,"is-loading":T.value,"empty-state":{title:"No Data",message:`There are no ${s.value.name} policies present.`},"table-data":h.value,"table-data-is-empty":h.value.data.length===0,next:S.value,"page-offset":A.value,onTableAction:K,onLoadData:x},{additionalControls:l(()=>[r(t(G),{label:"Policies",items:U.value,"label-attributes":{class:"visually-hidden"},appearance:"select","enable-filtering":!0,onSelected:O},{"item-template":l(({item:m})=>[y("span",{class:L({"policy-type-empty":R.value.includes(m.label)})},ee(m.label),3)]),_:1},8,["items"]),c(),r(de,{href:`${t(k)("KUMA_DOCS_URL")}/policies/${s.value.path}/?${t(k)("KUMA_UTM_QUERY_PARAMS")}`,"data-testid":"policy-documentation-link"},null,8,["href"])]),default:l(()=>[c(`
              >
              `)]),_:1},8,["selected-entity-name","page-size","error","is-loading","empty-state","table-data","table-data-is-empty","next","page-offset"])]),c(),v.value!==null?(_(),P(se,{key:0,name:v.value,mesh:M.value,path:s.value.path,type:s.value.name},null,8,["name","mesh","path","type"])):N("",!0)])],2)):N("",!0)]),_:1})]}),_:1}))}});const Me=pe(ve,[["__scopeId","data-v-32be22cb"]]);export{Me as default};
