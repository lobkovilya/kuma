import{d as $,t as q,r as B,o as e,j as m,ax as L,ay as S,m as w,l as u,h as r,w as l,g as s,E as f,s as k,au as b,F as v,G as g,D as C,p as x,L as P,R as V,i as d,a3 as N,A,a9 as E,C as F,_ as R}from"./index-18fd9432.js";import{_ as j}from"./ResourceCodeBlock.vue_vue_type_style_index_0_lang-9d8f1f28.js";import"./CodeBlock.vue_vue_type_style_index_0_lang-981e85cd.js";import"./toYaml-4e00099e.js";const I={key:3,"data-testid":"affected-data-plane-proxies"},G=$({__name:"PolicyConnections",props:{mesh:{type:String,required:!0},policyPath:{type:String,required:!0},policyName:{type:String,required:!0}},setup(h){const a=h,c=q("");return(y,t)=>{const o=B("RouterLink");return e(),m(v,null,[L(w("input",{id:"dataplane-search","onUpdate:modelValue":t[0]||(t[0]=i=>c.value=i),type:"text",class:"k-input mt-4",placeholder:"Filter by name",required:"","data-testid":"dataplane-search-input"},null,512),[[S,c.value]]),u(),r(x,{src:`/meshes/${a.mesh}/policy-path/${a.policyPath}/policy/${a.policyName}/dataplanes`},{default:l(({data:i,error:n})=>[n?(e(),s(f,{key:0,error:n},null,8,["error"])):i===void 0?(e(),s(k,{key:1})):i.items.length===0?(e(),s(b,{key:2})):(e(),m("ul",I,[(e(!0),m(v,null,g(i.items.filter(p=>p.dataplane.name.toLowerCase().includes(c.value.toLowerCase())),(p,_)=>(e(),m("li",{key:_,"data-testid":"dataplane-name"},[r(o,{to:{name:"data-plane-detail-view",params:{mesh:p.dataplane.mesh,dataPlane:p.dataplane.name}}},{default:l(()=>[u(C(p.dataplane.name),1)]),_:2},1032,["to"])]))),128))]))]),_:1},8,["src"])],64)}}}),K={class:"stack"},M=$({__name:"PolicyDetails",props:{policy:{type:Object,required:!0},path:{type:String,required:!0}},setup(h){const a=h,{t:c}=P(),y=V();return(t,o)=>(e(),m("div",K,[r(d(N),null,{body:l(()=>[w("h2",null,C(d(c)("policies.detail.affected_dpps")),1),u(),r(G,{mesh:a.policy.mesh,"policy-name":a.policy.name,"policy-path":a.path},null,8,["mesh","policy-name","policy-path"])]),_:1}),u(),r(j,{id:"code-block-policy",resource:a.policy,"resource-fetcher":i=>d(y).getSinglePolicyEntity({name:a.policy.name,mesh:a.policy.mesh,path:a.path},i),"is-searchable":""},null,8,["resource","resource-fetcher"])]))}}),D=$({__name:"PolicyDetailView",setup(h){const{t:a}=P();return(c,y)=>(e(),s(R,{name:"policy-detail-view","data-testid":"policy-detail-view"},{default:l(({route:t})=>[r(x,{src:"/*/policy-types"},{default:l(({data:o,error:i})=>[i?(e(),s(f,{key:0,error:i},null,8,["error"])):o===void 0?(e(),s(k,{key:1})):o.policies.length===0?(e(),s(b,{key:2})):(e(!0),m(v,{key:3},g([o.policies.find(n=>n.path===t.params.policyPath)??o.policies[0]],n=>(e(),s(A,{key:n.name,breadcrumbs:[{to:{name:"mesh-detail-view",params:{mesh:t.params.mesh}},text:t.params.mesh},{to:{name:"policies-list-view",params:{mesh:t.params.mesh,policyPath:t.params.policyPath}},text:d(a)("policies.routes.item.breadcrumbs")}]},{title:l(()=>[w("h1",null,[r(E,{text:t.params.policy},{default:l(()=>[r(F,{title:d(a)("policies.routes.item.title",{name:t.params.policy}),render:!0},null,8,["title"])]),_:2},1032,["text"])])]),default:l(()=>[u(),r(x,{src:`/meshes/${t.params.mesh}/policy-path/${n.path}/policy/${t.params.policy}`},{default:l(({data:p,error:_})=>[_?(e(),s(f,{key:0,error:_},null,8,["error"])):p===void 0?(e(),s(k,{key:1})):(e(),s(M,{key:2,policy:p,path:n.path,"data-testid":"detail-view-details"},null,8,["policy","path"]))]),_:2},1032,["src"])]),_:2},1032,["breadcrumbs"]))),128))]),_:2},1024)]),_:1}))}});export{D as default};
