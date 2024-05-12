import{_ as R,a as h,o,b as v,w as a,m as s,r as N,f as t,d as Y,l as at,q as I,e as d,t as i,p as l,c as u,F as f,G as w,n as nt,C as st,H as ot,R as H,a7 as C,a8 as it,a9 as dt,K as Z,T as lt,aa as rt,ab as W,ac as ct,ad as ut,ae as pt,E as _t,v as mt,x as ft}from"./index-OR-01d54.js";import{S as yt}from"./StatusBadge-DuxbgF-X.js";import{S as vt}from"./SummaryView-DprmWt7z.js";import{T as tt}from"./TagList-CLGP-G8u.js";import{_ as gt}from"./SubscriptionList.vue_vue_type_script_setup_true_lang-D9h22oCL.js";import"./AccordionList-DAiOY_DD.js";const ht=["B","kB","MB","GB","TB","PB","EB","ZB","YB"],bt=["B","KiB","MiB","GiB","TiB","PiB","EiB","ZiB","YiB"],kt=["b","kbit","Mbit","Gbit","Tbit","Pbit","Ebit","Zbit","Ybit"],xt=["b","kibit","Mibit","Gibit","Tibit","Pibit","Eibit","Zibit","Yibit"],J=(c,n,e)=>{let x=c;return typeof n=="string"||Array.isArray(n)?x=c.toLocaleString(n,e):(n===!0||e!==void 0)&&(x=c.toLocaleString(void 0,e)),x};function z(c,n){if(!Number.isFinite(c))throw new TypeError(`Expected a finite number, got ${typeof c}: ${c}`);n={bits:!1,binary:!1,space:!0,...n};const e=n.bits?n.binary?xt:kt:n.binary?bt:ht,x=n.space?" ":"";if(n.signed&&c===0)return` 0${x}${e[0]}`;const T=c<0,K=T?"-":n.signed?"+":"";T&&(c=-c);let b;if(n.minimumFractionDigits!==void 0&&(b={minimumFractionDigits:n.minimumFractionDigits}),n.maximumFractionDigits!==void 0&&(b={maximumFractionDigits:n.maximumFractionDigits,...b}),c<1){const q=J(c,n.locale,b);return K+q+x+e[0]}const $=Math.min(Math.floor(n.binary?Math.log(c)/Math.log(1024):Math.log10(c)/3),e.length-1);c/=(n.binary?1024:1e3)**$,b||(c=c.toPrecision(3));const S=J(Number(c),n.locale,b),E=e[$];return K+S+x+E}const $t={},wt={class:"card"},Bt={class:"title"},Ct={class:"body"};function Tt(c,n){const e=h("KCard");return o(),v(e,{class:"data-card"},{default:a(()=>[s("dl",null,[s("div",wt,[s("dt",Bt,[N(c.$slots,"title",{},void 0,!0)]),t(),s("dd",Ct,[N(c.$slots,"default",{},void 0,!0)])])])]),_:3})}const et=R($t,[["render",Tt],["__scopeId","data-v-3f9a3cf3"]]),It={class:"title"},St={key:0},Dt={"data-testid":"grpc-success"},Kt={"data-testid":"grpc-failure"},Nt={"data-testid":"rq-2xx"},Et={"data-testid":"rq-4xx"},qt={"data-testid":"rq-5xx"},Vt={"data-testid":"connections-total"},Rt={key:0,"data-testid":"bytes-received"},Mt={key:1,"data-testid":"bytes-sent"},Pt=Y({__name:"ConnectionCard",props:{protocol:{},service:{default:""},traffic:{default:void 0},direction:{default:"downstream"}},setup(c){const{t:n}=at(),e=c,x=T=>{const K=T.target;if(T.isTrusted&&K.nodeName.toLowerCase()!=="a"){const b=K.closest(".service-traffic-card, a");if(b){const $=b.nodeName.toLowerCase()==="a"?b:b.querySelector("[data-action]");$!==null&&"click"in $&&typeof $.click=="function"&&$.click()}}};return(T,K)=>{const b=h("KBadge"),$=h("KSkeletonBox");return o(),v(et,{class:"service-traffic-card",onClick:x},{title:a(()=>[e.service.length>0?(o(),v(tt,{key:0,tags:[{label:"kuma.io/service",value:e.service}]},null,8,["tags"])):I("",!0),t(),s("div",It,[d(b,{class:"protocol",appearance:e.protocol==="passthrough"?"success":"info"},{default:a(()=>[t(i(l(n)(`data-planes.components.service_traffic_card.protocol.${e.protocol}`,{},{defaultMessage:l(n)(`http.api.value.${e.protocol}`)})),1)]),_:1},8,["appearance"]),t(),N(T.$slots,"default",{},void 0,!0)])]),default:a(()=>{var S,E,q,M,P,L,U,A,F,O,k,_;return[t(),e.traffic?(o(),u("dl",St,[e.protocol==="passthrough"?(o(!0),u(f,{key:0},w([["http","tcp"].reduce((p,D)=>{var r;const G=e.direction;return Object.entries(((r=e.traffic)==null?void 0:r[D])||{}).reduce((m,[y,g])=>[`${G}_cx_tx_bytes_total`,`${G}_cx_rx_bytes_total`].includes(y)?{...m,[y]:g+(m[y]??0)}:m,p)},{})],(p,D)=>(o(),u(f,{key:D},[s("div",null,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.tx")),1),t(),s("dd",null,i(l(z)(p.downstream_cx_rx_bytes_total??0)),1)]),t(),s("div",null,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.rx")),1),t(),s("dd",null,i(l(z)(p.downstream_cx_tx_bytes_total??0)),1)])],64))),128)):e.protocol==="grpc"?(o(),u(f,{key:1},[s("div",Dt,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.grpc_success")),1),t(),s("dd",null,i(l(n)("common.formats.integer",{value:(S=e.traffic.grpc)==null?void 0:S.success})),1)]),t(),s("div",Kt,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.grpc_failure")),1),t(),s("dd",null,i(l(n)("common.formats.integer",{value:(E=e.traffic.grpc)==null?void 0:E.failure})),1)])],64)):e.protocol.startsWith("http")?(o(),u(f,{key:2},[(o(!0),u(f,null,w([((q=e.traffic.http)==null?void 0:q[`${e.direction}_rq_1xx`])??0].filter(p=>p!==0),p=>(o(),u("div",{key:p,"data-testid":"rq-1xx"},[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.1xx")),1),t(),s("dd",null,i(l(n)("common.formats.integer",{value:p})),1)]))),128)),t(),s("div",Nt,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.2xx")),1),t(),s("dd",null,i(l(n)("common.formats.integer",{value:(M=e.traffic.http)==null?void 0:M[`${e.direction}_rq_2xx`]})),1)]),t(),(o(!0),u(f,null,w([((P=e.traffic.http)==null?void 0:P[`${e.direction}_rq_3xx`])??0].filter(p=>p!==0),p=>(o(),u("div",{key:p,"data-testid":"rq-3xx"},[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.3xx")),1),t(),s("dd",null,i(l(n)("common.formats.integer",{value:p})),1)]))),128)),t(),s("div",Et,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.4xx")),1),t(),s("dd",null,i(l(n)("common.formats.integer",{value:(L=e.traffic.http)==null?void 0:L[`${e.direction}_rq_4xx`]})),1)]),t(),s("div",qt,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.5xx")),1),t(),s("dd",null,i(l(n)("common.formats.integer",{value:(U=e.traffic.http)==null?void 0:U[`${e.direction}_rq_5xx`]})),1)])],64)):(o(),u(f,{key:3},[s("div",Vt,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.cx")),1),t(),s("dd",null,i(l(n)("common.formats.integer",{value:(A=e.traffic.tcp)==null?void 0:A[`${e.direction}_cx_total`]})),1)]),t(),typeof((F=e.traffic.tcp)==null?void 0:F[`${e.direction}_cx_tx_bytes_total`])<"u"?(o(),u("div",Rt,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.rx")),1),t(),s("dd",null,i(l(z)((O=e.traffic.tcp)==null?void 0:O[`${e.direction}_cx_tx_bytes_total`])),1)])):I("",!0),t(),typeof((k=e.traffic.tcp)==null?void 0:k[`${e.direction}_cx_rx_bytes_total`])<"u"?(o(),u("div",Mt,[s("dt",null,i(l(n)("data-planes.components.service_traffic_card.tx")),1),t(),s("dd",null,i(l(z)((_=e.traffic.tcp)==null?void 0:_[`${e.direction}_cx_rx_bytes_total`])),1)])):I("",!0)],64))])):(o(),v($,{key:1,width:"10"}))]}),_:3})}}}),j=R(Pt,[["__scopeId","data-v-f7ef7711"]]),Lt={class:"body"},Ut=Y({__name:"ConnectionGroup",props:{type:{}},setup(c){const n=c;return(e,x)=>{const T=h("KCard");return o(),v(T,{class:nt(["service-traffic-group",`type-${n.type}`])},{default:a(()=>[s("div",Lt,[N(e.$slots,"default",{},void 0,!0)])]),_:3},8,["class"])}}}),X=R(Ut,[["__scopeId","data-v-9402d5d1"]]),At={class:"service-traffic"},Ft={class:"actions"},Ot=Y({__name:"ConnectionTraffic",setup(c){return(n,e)=>(o(),u("div",At,[s("div",Ft,[N(n.$slots,"actions",{},void 0,!0)]),t(),d(et,{class:"header"},{title:a(()=>[N(n.$slots,"title",{},void 0,!0)]),_:3}),t(),N(n.$slots,"default",{},void 0,!0)]))}}),Q=R(Ot,[["__scopeId","data-v-e6bd176c"]]),Gt=c=>(mt("data-v-e28eeed0"),c=c(),ft(),c),zt={"data-testid":"dataplane-warnings"},Yt=["data-testid","innerHTML"],Ht={key:0,"data-testid":"warning-stats-loading"},Zt={class:"stack","data-testid":"dataplane-details"},jt={class:"columns"},Xt={class:"status-with-reason"},Wt={class:"columns"},Jt=Gt(()=>s("span",null,"Outbounds",-1)),Qt={"data-testid":"dataplane-mtls"},te={class:"columns"},ee=["innerHTML"],ae={key:0,"data-testid":"dataplane-subscriptions"},ne=Y({__name:"DataPlaneDetailView",props:{data:{}},setup(c){const n=st(),e=c,x=ot(()=>e.data.warnings.concat(...e.data.isCertExpired?[{kind:"CERT_EXPIRED"}]:[]));return(T,K)=>{const b=h("KTooltip"),$=h("DataCollection"),S=h("KCard"),E=h("XAction"),q=h("KInputSwitch"),M=h("KButton"),P=h("RouterLink"),L=h("RouterView"),U=h("KAlert"),A=h("AppView"),F=h("DataSource"),O=h("RouteView");return o(),v(O,{params:{mesh:"",dataPlane:"",inactive:!1},name:"data-plane-detail-view"},{default:a(({route:k,t:_})=>[d(F,{src:`/meshes/${k.params.mesh}/dataplanes/${k.params.dataPlane}/stats/${e.data.dataplane.networking.inboundAddress}`},{default:a(({data:p,error:D,refresh:G})=>[d(A,null,H({default:a(()=>[t(),s("div",Zt,[d(S,null,{default:a(()=>[s("div",jt,[d(C,null,{title:a(()=>[t(i(_("http.api.property.status")),1)]),body:a(()=>[s("div",Xt,[d(yt,{status:e.data.status},null,8,["status"]),t(),e.data.dataplaneType==="standard"?(o(),v($,{key:0,items:e.data.dataplane.networking.inbounds,predicate:r=>!r.health.ready,empty:!1},{default:a(({items:r})=>[d(b,{class:"reason-tooltip"},{content:a(()=>[s("ul",null,[(o(!0),u(f,null,w(r,m=>(o(),u("li",{key:`${m.service}:${m.port}`},i(_("data-planes.routes.item.unhealthy_inbound",{service:m.service,port:m.port})),1))),128))])]),default:a(()=>[d(l(it),{color:l(dt),size:l(Z)},null,8,["color","size"]),t()]),_:2},1024)]),_:2},1032,["items","predicate"])):I("",!0)])]),_:2},1024),t(),d(C,null,{title:a(()=>[t(`
                  Type
                `)]),body:a(()=>[t(i(_(`data-planes.type.${e.data.dataplaneType}`)),1)]),_:2},1024),t(),e.data.namespace.length>0?(o(),v(C,{key:0},{title:a(()=>[t(`
                  Namespace
                `)]),body:a(()=>[t(i(e.data.namespace),1)]),_:1})):I("",!0),t(),d(C,null,{title:a(()=>[t(i(_("data-planes.routes.item.last_updated")),1)]),body:a(()=>[t(i(_("common.formats.datetime",{value:Date.parse(e.data.modificationTime)})),1)]),_:2},1024),t(),e.data.dataplane.networking.gateway?(o(),u(f,{key:1},[d(C,null,{title:a(()=>[t(i(_("http.api.property.tags")),1)]),body:a(()=>[d(tt,{tags:e.data.dataplane.networking.gateway.tags},null,8,["tags"])]),_:2},1024),t(),d(C,null,{title:a(()=>[t(i(_("http.api.property.address")),1)]),body:a(()=>[d(lt,{text:`${e.data.dataplane.networking.address}`},null,8,["text"])]),_:2},1024)],64)):I("",!0)])]),_:2},1024),t(),d(S,{class:"traffic","data-testid":"dataplane-traffic"},{default:a(()=>[s("div",Wt,[d(Q,null,{title:a(()=>[d(l(rt),{display:"inline-block",decorative:"",size:l(Z)},null,8,["size"]),t(`
                  Inbounds
                `)]),default:a(()=>[t(),d(X,{type:"inbound","data-testid":"dataplane-inbounds"},{default:a(()=>[(o(!0),u(f,null,w([e.data.dataplane.networking.type==="gateway"?Object.entries((p==null?void 0:p.inbounds)??{}).reduce((r,[m,y])=>{var B;const g=m.split("_").at(-1);return g===(((B=e.data.dataplane.networking.admin)==null?void 0:B.port)??"9901")?r:r.concat([{...e.data.dataplane.networking.inbounds[0],name:m,port:Number(g),protocol:["http","tcp"].find(V=>typeof y[V]<"u")??"tcp",addressPort:`${e.data.dataplane.networking.inbounds[0].address}:${g}`}])},[]):e.data.dataplane.networking.inbounds],r=>(o(),v($,{key:r,items:r,predicate:m=>m.port!==49151},H({default:a(({items:m})=>[(o(!0),u(f,null,w(m,y=>(o(),u(f,{key:`${y.name}`},[(o(!0),u(f,null,w([p==null?void 0:p.inbounds[y.name]],g=>(o(),v(j,{key:g,"data-testid":"dataplane-inbound",protocol:y.protocol,service:y.tags["kuma.io/service"],traffic:typeof D>"u"?g:{name:"",protocol:y.protocol,port:`${y.port}`}},{default:a(()=>[d(E,{"data-action":"",to:{name:(B=>B.includes("bound")?B.replace("-outbound-","-inbound-"):"connection-inbound-summary-overview-view")(String(l(n).name)),params:{connection:y.name},query:{inactive:k.params.inactive}}},{default:a(()=>[t(i(y.name.replace("localhost","").replace("_",":")),1)]),_:2},1032,["to"])]),_:2},1032,["protocol","service","traffic"]))),128))],64))),128))]),_:2},[e.data.dataplaneType==="delegated"?{name:"empty",fn:a(()=>[d(W,null,{default:a(()=>[t(`
                          This proxy is a delegated gateway therefore `+i(_("common.product.name"))+` does not have any visibility into inbounds for this gateway
                        `,1)]),_:2},1024)]),key:"0"}:void 0]),1032,["items","predicate"]))),128))]),_:2},1024)]),_:2},1024),t(),d(Q,null,H({title:a(()=>[d(l(ct),{display:"inline-block",decorative:"",size:l(Z)},null,8,["size"]),t(),Jt]),default:a(()=>[t(),t(),typeof D>"u"?(o(),u(f,{key:0},[typeof p>"u"?(o(),v(ut,{key:0})):(o(),u(f,{key:1},w(["upstream"],r=>(o(),u(f,{key:r},[d(X,{type:"passthrough"},{default:a(()=>[d(j,{protocol:"passthrough",traffic:p.passthrough},{default:a(()=>[t(`
                          Non mesh traffic
                        `)]),_:2},1032,["traffic"])]),_:2},1024),t(),d($,{predicate:k.params.inactive?void 0:([m,y])=>{var g,B;return((typeof y.tcp<"u"?(g=y.tcp)==null?void 0:g[`${r}_cx_rx_bytes_total`]:(B=y.http)==null?void 0:B[`${r}_rq_total`])??0)>0},items:Object.entries(p.outbounds)},{default:a(({items:m})=>[m.length>0?(o(),v(X,{key:0,type:"outbound","data-testid":"dataplane-outbounds"},{default:a(()=>[(o(),u(f,null,w([/-([a-f0-9]){16}$/],y=>(o(),u(f,{key:y},[(o(!0),u(f,null,w(m,([g,B])=>(o(),v(j,{key:`${g}`,"data-testid":"dataplane-outbound",protocol:["grpc","http","tcp"].find(V=>typeof B[V]<"u")??"tcp",traffic:B,service:g.replace(y,""),direction:r},{default:a(()=>[d(P,{"data-action":"",to:{name:(V=>V.includes("bound")?V.replace("-inbound-","-outbound-"):"connection-outbound-summary-overview-view")(String(l(n).name)),params:{connection:g},query:{inactive:k.params.inactive?null:void 0}}},{default:a(()=>[t(i(g),1)]),_:2},1032,["to"])]),_:2},1032,["protocol","traffic","service","direction"]))),128))],64))),64))]),_:2},1024)):I("",!0)]),_:2},1032,["predicate","items"])],64))),64))],64)):(o(),v(W,{key:1}))]),_:2},[p?{name:"actions",fn:a(()=>[d(q,{modelValue:k.params.inactive,"onUpdate:modelValue":r=>k.params.inactive=r,"data-testid":"dataplane-outbounds-inactive-toggle"},{label:a(()=>[t(`
                      Show inactive
                    `)]),_:2},1032,["modelValue","onUpdate:modelValue"]),t(),d(M,{appearance:"primary",onClick:G},{default:a(()=>[d(l(pt)),t(`

                    Refresh
                  `)]),_:2},1032,["onClick"])]),key:"0"}:void 0]),1024)])]),_:2},1024),t(),d(L,null,{default:a(r=>[r.route.name!==k.name?(o(),v(vt,{key:0,width:"670px",onClose:function(m){k.replace({name:"data-plane-detail-view",params:{mesh:k.params.mesh,dataPlane:k.params.dataPlane},query:{inactive:k.params.inactive?null:void 0}})}},{default:a(()=>[(o(),v(_t(r.Component),{data:r.route.name.includes("-inbound-")?e.data.dataplane.networking.inbounds:(p==null?void 0:p.outbounds)||{},"dataplane-overview":e.data},null,8,["data","dataplane-overview"]))]),_:2},1032,["onClose"])):I("",!0)]),_:2},1024),t(),s("div",Qt,[s("h2",null,i(_("data-planes.routes.item.mtls.title")),1),t(),e.data.dataplaneInsight.mTLS?(o(!0),u(f,{key:0},w([e.data.dataplaneInsight.mTLS],r=>(o(),v(S,{key:r,class:"mt-4"},{default:a(()=>[s("div",te,[d(C,null,{title:a(()=>[t(i(_("data-planes.routes.item.mtls.expiration_time.title")),1)]),body:a(()=>[t(i(_("common.formats.datetime",{value:Date.parse(r.certificateExpirationTime)})),1)]),_:2},1024),t(),d(C,null,{title:a(()=>[t(i(_("data-planes.routes.item.mtls.generation_time.title")),1)]),body:a(()=>[t(i(_("common.formats.datetime",{value:Date.parse(r.lastCertificateRegeneration)})),1)]),_:2},1024),t(),d(C,null,{title:a(()=>[t(i(_("data-planes.routes.item.mtls.regenerations.title")),1)]),body:a(()=>[t(i(_("common.formats.integer",{value:r.certificateRegenerations})),1)]),_:2},1024),t(),d(C,null,{title:a(()=>[t(i(_("data-planes.routes.item.mtls.issued_backend.title")),1)]),body:a(()=>[t(i(r.issuedBackend),1)]),_:2},1024),t(),d(C,null,{title:a(()=>[t(i(_("data-planes.routes.item.mtls.supported_backends.title")),1)]),body:a(()=>[s("ul",null,[(o(!0),u(f,null,w(r.supportedBackends,m=>(o(),u("li",{key:m},i(m),1))),128))])]),_:2},1024)])]),_:2},1024))),128)):(o(),v(U,{key:1,class:"mt-4",appearance:"warning"},{default:a(()=>[s("div",{innerHTML:_("data-planes.routes.item.mtls.disabled")},null,8,ee)]),_:2},1024))]),t(),e.data.dataplaneInsight.subscriptions.length>0?(o(),u("div",ae,[s("h2",null,i(_("data-planes.routes.item.subscriptions.title")),1),t(),d(S,{class:"mt-4"},{default:a(()=>[d(gt,{subscriptions:e.data.dataplaneInsight.subscriptions},null,8,["subscriptions"])]),_:1})])):I("",!0)])]),_:2},[x.value.length>0||D?{name:"notifications",fn:a(()=>[s("ul",zt,[(o(!0),u(f,null,w(x.value,r=>(o(),u("li",{key:r.kind,"data-testid":`warning-${r.kind}`,innerHTML:_(`common.warnings.${r.kind}`,r.payload)},null,8,Yt))),128)),t(),D?(o(),u("li",Ht,[t(`
              The below view is not enhanced with runtime stats (Error loading stats: `),s("strong",null,i(D.toString()),1),t(`)
            `)])):I("",!0)])]),key:"0"}:void 0]),1024)]),_:2},1032,["src"])]),_:1})}}}),ce=R(ne,[["__scopeId","data-v-e28eeed0"]]);export{ce as default};
