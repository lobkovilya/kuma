import{_ as E}from"./ResourceCodeBlock.vue_vue_type_script_setup_true_lang-C0ljvhak.js";import{d as w,a as i,o as r,b as n,w as e,e as o,a0 as V,ad as R,c as S,m as p,a7 as h,f as c,t as m,T as B,q as f}from"./index-OR-01d54.js";import{T as $}from"./TagList-CLGP-G8u.js";import"./CodeBlock-DscLfGOS.js";import"./toYaml-DB9FPXFY.js";const b={key:2,class:"stack"},D={class:"columns"},L=w({__name:"ExternalServiceDetailView",setup(F){return(T,M)=>{const x=i("KCard"),_=i("DataSource"),v=i("AppView"),C=i("RouteView");return r(),n(C,{name:"external-service-detail-view",params:{mesh:"",service:"",codeSearch:"",codeFilter:!1,codeRegExp:!1}},{default:e(({route:a,t:l})=>[o(v,null,{default:e(()=>[o(_,{src:`/meshes/${a.params.mesh}/external-services/${a.params.service}`},{default:e(({data:t,error:u})=>[u?(r(),n(V,{key:0,error:u},null,8,["error"])):t===void 0?(r(),n(R,{key:1})):(r(),S("div",b,[o(x,{"data-testid":"external-service-details"},{default:e(()=>[p("div",D,[o(h,null,{title:e(()=>[c(m(l("http.api.property.address")),1)]),body:e(()=>[o(B,{text:t.networking.address},null,8,["text"])]),_:2},1024),c(),t.tags?(r(),n(h,{key:0},{title:e(()=>[c(m(l("http.api.property.tags")),1)]),body:e(()=>[o($,{tags:t.tags},null,8,["tags"])]),_:2},1024)):f("",!0)])]),_:2},1024),c(),p("div",null,[p("h3",null,m(l("external-services.detail.config")),1),c(),o(E,{class:"mt-4","data-testid":"external-service-config",resource:t.config,"is-searchable":"",query:a.params.codeSearch,"is-filter-mode":a.params.codeFilter,"is-reg-exp-mode":a.params.codeRegExp,onQueryChange:s=>a.update({codeSearch:s}),onFilterModeChange:s=>a.update({codeFilter:s}),onRegExpModeChange:s=>a.update({codeRegExp:s})},{default:e(({copy:s,copying:y})=>[y?(r(),n(_,{key:0,src:`/meshes/${t.mesh}/external-services/${t.name}/as/kubernetes?no-store`,onChange:d=>{s(g=>g(d))},onError:d=>{s((g,k)=>k(d))}},null,8,["src","onChange","onError"])):f("",!0)]),_:2},1032,["resource","query","is-filter-mode","is-reg-exp-mode","onQueryChange","onFilterModeChange","onRegExpModeChange"])])]))]),_:2},1032,["src"])]),_:2},1024)]),_:1})}}});export{L as default};
