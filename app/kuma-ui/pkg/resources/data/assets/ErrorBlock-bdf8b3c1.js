import{l as b,Q as g,P as _}from"./kongponents.es-208ec824.js";import{d as y,c as v,ax as p,o as a,f as s,i as n,m as w,b as c,l as o,h as r,t,g as d,F as B,k as E,e as l,q as x,a as S,p as C,n as I}from"./index-6fad8e68.js";import{h as N}from"./RouteView.vue_vue_type_script_setup_true_lang-d7cfbc23.js";const f=e=>(C("data-v-176c6beb"),e=e(),I(),e),V={class:"error-block"},q=f(()=>o("p",null,"An error has occurred while trying to load this data.",-1)),A={class:"error-block-details"},D=f(()=>o("summary",null,"Details",-1)),F={key:0},P={key:0,class:"badge-list"},Q=y({__name:"ErrorBlock",props:{error:{type:[Error,null],required:!1,default:null}},setup(e){const i=e,u=v(()=>i.error instanceof p?i.error.causes:[]);return(h,z)=>(a(),s("div",V,[n(l(g),{"cta-is-hidden":""},w({title:c(()=>[n(l(b),{class:"mb-3",icon:"warning",color:"var(--black-500)","secondary-color":"var(--yellow-300)",size:"42"}),r(),x(h.$slots,"default",{},()=>[q],!0)]),_:2},[e.error!==null||u.value.length>0?{name:"message",fn:c(()=>[o("details",A,[D,r(),e.error!==null?(a(),s("p",F,t(e.error.message),1)):d("",!0),r(),o("ul",null,[(a(!0),s(B,null,E(u.value,(m,k)=>(a(),s("li",{key:k},[o("b",null,[o("code",null,t(m.field),1)]),r(": "+t(m.message),1)]))),128))])])]),key:"0"}:void 0]),1024),r(),e.error instanceof l(p)?(a(),s("div",P,[e.error.code?(a(),S(l(_),{key:0,appearance:"warning"},{default:c(()=>[r(t(e.error.code),1)]),_:1})):d("",!0),r(),n(l(_),{appearance:"warning"},{default:c(()=>[r(t(e.error.statusCode),1)]),_:1})])):d("",!0)]))}});const j=N(Q,[["__scopeId","data-v-176c6beb"]]);export{j as E};
