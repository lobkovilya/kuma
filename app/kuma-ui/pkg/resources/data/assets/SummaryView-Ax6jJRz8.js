import{ar as x,p as E,as as C,at as P,ag as I,d as L,au as j,D,a as g,o as V,b as W,w as b,e as R,f as F,r as M,an as N,_ as $}from"./index-BWbq5iYQ.js";function B(e){return C()?(P(e),!0):!1}function S(e){return typeof e=="function"?e():E(e)}const k=typeof window<"u"&&typeof document<"u";typeof WorkerGlobalScope<"u"&&globalThis instanceof WorkerGlobalScope;const q=Object.prototype.toString,G=e=>q.call(e)==="[object Object]",w=()=>{},K=X();function X(){var e,r;return k&&((e=window==null?void 0:window.navigator)==null?void 0:e.userAgent)&&(/iP(ad|hone|od)/.test(window.navigator.userAgent)||((r=window==null?void 0:window.navigator)==null?void 0:r.maxTouchPoints)>2&&/iPad|Macintosh/.test(window==null?void 0:window.navigator.userAgent))}function z(e,r){function o(...t){return new Promise((i,l)=>{Promise.resolve(e(()=>r.apply(this,t),{fn:r,thisArg:this,args:t})).then(i).catch(l)})}return o}function H(...e){let r=0,o,t=!0,i=w,l,c,a,f,p;!x(e[0])&&typeof e[0]=="object"?{delay:c,trailing:a=!0,leading:f=!0,rejectOnCancel:p=!1}=e[0]:[c,a=!0,f=!0,p=!1]=e;const d=()=>{o&&(clearTimeout(o),o=void 0,i(),i=w)};return n=>{const s=S(c),u=Date.now()-r,v=()=>l=n();return d(),s<=0?(r=Date.now(),v()):(u>s&&(f||!t)?(r=Date.now(),v()):a&&(l=new Promise((_,T)=>{i=p?T:_,o=setTimeout(()=>{r=Date.now(),t=!0,_(v()),d()},Math.max(0,s-u))})),!f&&!o&&(o=setTimeout(()=>t=!0,s)),t=!1,l)}}function J(e,r=200,o=!1,t=!0,i=!1){return z(H(r,o,t,i),e)}function h(e){var r;const o=S(e);return(r=o==null?void 0:o.$el)!=null?r:o}const O=k?window:void 0;function y(...e){let r,o,t,i;if(typeof e[0]=="string"||Array.isArray(e[0])?([o,t,i]=e,r=O):[r,o,t,i]=e,!r)return w;Array.isArray(o)||(o=[o]),Array.isArray(t)||(t=[t]);const l=[],c=()=>{l.forEach(d=>d()),l.length=0},a=(d,m,n,s)=>(d.addEventListener(m,n,s),()=>d.removeEventListener(m,n,s)),f=I(()=>[h(r),S(i)],([d,m])=>{if(c(),!d)return;const n=G(m)?{...m}:m;l.push(...o.flatMap(s=>t.map(u=>a(d,s,u,n))))},{immediate:!0,flush:"post"}),p=()=>{f(),c()};return B(p),p}let A=!1;function Q(e,r,o={}){const{window:t=O,ignore:i=[],capture:l=!0,detectIframe:c=!1}=o;if(!t)return w;K&&!A&&(A=!0,Array.from(t.document.body.children).forEach(n=>n.addEventListener("click",w)),t.document.documentElement.addEventListener("click",w));let a=!0;const f=n=>i.some(s=>{if(typeof s=="string")return Array.from(t.document.querySelectorAll(s)).some(u=>u===n.target||n.composedPath().includes(u));{const u=h(s);return u&&(n.target===u||n.composedPath().includes(u))}}),d=[y(t,"click",n=>{const s=h(e);if(!(!s||s===n.target||n.composedPath().includes(s))){if(n.detail===0&&(a=!f(n)),!a){a=!0;return}r(n)}},{passive:!0,capture:l}),y(t,"pointerdown",n=>{const s=h(e);a=!f(n)&&!!(s&&!n.composedPath().includes(s))},{passive:!0}),c&&y(t,"blur",n=>{setTimeout(()=>{var s;const u=h(e);((s=t.document.activeElement)==null?void 0:s.tagName)==="IFRAME"&&!(u!=null&&u.contains(t.document.activeElement))&&r(n)},0)})].filter(Boolean);return()=>d.forEach(n=>n())}const U=L({__name:"SummaryView",props:{width:{default:"560px"}},emits:["close"],setup(e,{emit:r}){const o=N("summary-view-title");j("app-summary-view",o);const t=D(null);Q(t,J(c=>{const a=c.target;c.isTrusted&&a.nodeName.toLowerCase()!=="a"&&l("close")},1,!0,!1));const i=e,l=r;return(c,a)=>{const f=g("XTeleportSlot"),p=g("KSlideout");return V(),W(p,{ref_key:"slideOutRef",ref:t,class:"summary-slideout","close-on-blur":!1,"has-overlay":!1,visible:"","max-width":i.width,"offset-top":"var(--app-slideout-offset-top, 0)","data-testid":"summary",onClose:a[0]||(a[0]=d=>l("close"))},{title:b(()=>[R(f,{name:E(o)},null,8,["name"])]),default:b(()=>[F(),M(c.$slots,"default",{},void 0,!0)]),_:3},8,["max-width"])}}}),Z=$(U,[["__scopeId","data-v-1aaecf49"]]);export{Z as S};