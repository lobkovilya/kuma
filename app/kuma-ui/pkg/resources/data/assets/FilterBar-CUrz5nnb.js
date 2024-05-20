var R=Object.defineProperty;var V=(e,n,s)=>n in e?R(e,n,{enumerable:!0,configurable:!0,writable:!0,value:s}):e[n]=s;var w=(e,n,s)=>(V(e,typeof n!="symbol"?n+"":n,s),s);import{d as U,D as g,af as G,H as F,aC as Q,aD as W,a as Y,o as p,c as m,e as T,w as Z,m as r,a3 as J,f as c,p as k,aE as X,K as B,r as ee,t as y,aF as te,aG as se,n as M,F as oe,G as ne,q as N,aH as ie,v as ae,x as re,_ as le}from"./index-Dlb3STpa.js";const ue=["ControlLeft","ControlRight","ShiftLeft","ShiftRight","AltLeft"];class ce{constructor(n,s){w(this,"commands");w(this,"keyMap");w(this,"boundTriggerShortcuts");this.commands=s,this.keyMap=Object.fromEntries(Object.entries(n).map(([h,v])=>[h.toLowerCase(),v])),this.boundTriggerShortcuts=this.triggerShortcuts.bind(this)}registerListener(){document.addEventListener("keydown",this.boundTriggerShortcuts)}unRegisterListener(){document.removeEventListener("keydown",this.boundTriggerShortcuts)}triggerShortcuts(n){de(n,this.keyMap,this.commands)}}function de(e,n,s){const h=fe(e.code),v=[e.ctrlKey?"ctrl":"",e.shiftKey?"shift":"",e.altKey?"alt":"",h].filter(b=>b!=="").join("+"),S=n[v];if(!S)return;const u=s[S];u.isAllowedContext&&!u.isAllowedContext(e)||(u.shouldPreventDefaultAction&&e.preventDefault(),!(u.isDisabled&&u.isDisabled())&&u.trigger(e))}function fe(e){return ue.includes(e)?"":e.replace(/^Key/,"").toLowerCase()}let j=0;const ge=(e="unique")=>(j++,`${e}-${j}`),pe=e=>(ae("data-v-7a0290e4"),e=e(),re(),e),me=pe(()=>r("span",{class:"visually-hidden"},"Focus filter",-1)),he={class:"filter-bar-icon"},ve=["for"],be=["id","placeholder"],ye={key:0,class:"suggestion-box","data-testid":"filter-bar-suggestion-box"},_e={class:"suggestion-list"},Se={key:0,class:"filter-bar-error"},we={key:0},Fe=["title","data-filter-field"],ke={class:"visually-hidden"},Ce=U({__name:"FilterBar",props:{fields:{},placeholder:{default:""},query:{default:""},id:{default:()=>ge("filter-bar")}},emits:["change"],setup(e,{emit:n}){const s=e,h=g(),v=n,S=t=>{t!=null&&t.target&&(v("change",new FormData(t.target)),d.value=!1)},u=t=>{v("change",new FormData(h.value))},b=g(null),l=g(null),I=g(null),d=g(!1),f=g(s.query);G(()=>s.query,t=>{f.value=t});const _=g(0),x=F(()=>Object.keys(s.fields)),A=F(()=>Object.entries(s.fields).slice(0,5).map(([t,o])=>({fieldName:t,...o}))),L=F(()=>x.value.length>0?`Filter by ${x.value.join(", ")}`:"Filter"),P=F(()=>s.placeholder??L.value),q={ArrowDown:"jumpToNextSuggestion",ArrowUp:"jumpToPreviousSuggestion"},E={jumpToNextSuggestion:{trigger:()=>$(1),isAllowedContext(t){return l.value!==null&&t.composedPath().includes(l.value)},shouldPreventDefaultAction:!0},jumpToPreviousSuggestion:{trigger:()=>$(-1),isAllowedContext(t){return l.value!==null&&t.composedPath().includes(l.value)},shouldPreventDefaultAction:!0}},D=new ce(q,E);Q(function(){D.registerListener()}),W(function(){D.unRegisterListener()});function $(t){const o=A.value.length;let a=_.value+t;a===-1&&(a=o),_.value=a%(o+1)}function K(){l.value instanceof HTMLInputElement&&l.value.focus()}function O(t){const a=t.currentTarget.getAttribute("data-filter-field");a&&l.value instanceof HTMLInputElement&&z(l.value,a)}function z(t,o){const a=f.value===""||f.value.endsWith(" ")?"":" ";f.value+=a+o+":",t.focus(),_.value=0}function H(t){t.relatedTarget===null&&(d.value=!1),b.value instanceof HTMLElement&&t.relatedTarget instanceof Node&&!b.value.contains(t.relatedTarget)&&(d.value=!1)}return(t,o)=>{const a=Y("search");return p(),m("div",{ref_key:"filterBar",ref:b,class:"filter-bar","data-testid":"filter-bar"},[T(a,null,{default:Z(()=>[r("form",{ref_key:"$form",ref:h,onSubmit:J(S,["prevent"])},[r("button",{class:"focus-filter-input-button",title:"Focus filter",type:"button","data-testid":"filter-bar-focus-filter-input-button",onClick:K},[me,c(),r("span",he,[T(k(X),{decorative:"","data-testid":"filter-bar-filter-icon","hide-title":"",size:k(B)},null,8,["size"])])]),c(),r("label",{for:`${s.id}-filter-bar-input`,class:"visually-hidden"},[ee(t.$slots,"default",{},()=>[c(y(L.value),1)],!0)],8,ve),c(),te(r("input",{id:`${s.id}-filter-bar-input`,ref_key:"filterInput",ref:l,"onUpdate:modelValue":o[0]||(o[0]=i=>f.value=i),class:"filter-bar-input",type:"search",placeholder:P.value,"data-testid":"filter-bar-filter-input",name:"s",onFocus:o[1]||(o[1]=i=>d.value=!0),onInput:o[2]||(o[2]=i=>d.value=!0),onBlur:H,onSearch:o[3]||(o[3]=i=>{i.target.value.length===0&&(u(i),d.value=!0)})},null,40,be),[[se,f.value]]),c(),d.value?(p(),m("div",ye,[r("div",_e,[I.value!==null?(p(),m("p",Se,y(I.value.message),1)):(p(),m("button",{key:1,type:"submit",class:M(["submit-query-button",{"submit-query-button-is-selected":_.value===0}]),"data-testid":"filter-bar-submit-query-button"},`
              Submit `+y(f.value),3)),c(),(p(!0),m(oe,null,ne(A.value,(i,C)=>(p(),m("div",{key:`${s.id}-${C}`,class:M(["suggestion-list-item",{"suggestion-list-item-is-selected":_.value===C+1}])},[r("b",null,y(i.fieldName),1),i.description!==""?(p(),m("span",we,": "+y(i.description),1)):N("",!0),c(),r("button",{class:"apply-suggestion-button",title:`Add ${i.fieldName}:`,type:"button","data-filter-field":i.fieldName,"data-testid":"filter-bar-apply-suggestion-button",onClick:O},[r("span",ke,"Add "+y(i.fieldName)+":",1),c(),T(k(ie),{decorative:"","hide-title":"",size:k(B)},null,8,["size"])],8,Fe)],2))),128))])])):N("",!0)],544)]),_:3})],512)}}}),xe=le(Ce,[["__scopeId","data-v-7a0290e4"]]);export{xe as F};