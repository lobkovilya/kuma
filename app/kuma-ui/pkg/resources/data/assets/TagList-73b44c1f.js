import{P as f}from"./kongponents.es-208ec824.js";import{d as b,u as h,D as g,c as y,o as r,f as l,F as k,k as x,a as i,b as p,j as L,h as T,t as m,l as w,e as B}from"./index-6fad8e68.js";import{h as j}from"./RouteView.vue_vue_type_script_setup_true_lang-d7cfbc23.js";function z(o){return Object.entries(o??{}).map(([s,a])=>({label:s,value:a}))}const C={class:"tag-list"},D=b({__name:"TagList",props:{tags:{type:Object,required:!0}},setup(o){const s=o,a=h(),c=g(),_=y(()=>(Array.isArray(s.tags)?s.tags:z(s.tags)).map(n=>{const{label:t,value:u}=n,v=d(n);return{label:t,value:u,route:v}}));function d(e){if(e.value!=="*")try{switch(e.label){case"kuma.io/zone":return c.resolve({name:"zone-cp-detail-view",params:{zone:e.value}});case"kuma.io/service":return"mesh"in a.params?c.resolve({name:"service-detail-view",params:{mesh:a.params.mesh,service:e.value}}):void 0;default:return}}catch{return}}return(e,n)=>(r(),l("span",C,[(r(!0),l(k,null,x(_.value,(t,u)=>(r(),i(B(f),{key:u,class:"tag-badge"},{default:p(()=>[(r(),i(L(t.route?"router-link":"span"),{to:t.route},{default:p(()=>[T(m(t.label)+":",1),w("b",null,m(t.value),1)]),_:2},1032,["to"]))]),_:2},1024))),128))]))}});const N=j(D,[["__scopeId","data-v-2e356c8b"]]);export{N as T};
