import{d as V,g,h as w,o as t,l as k,n as r,H as i,k as _,j as e,a0 as R,t as x,a5 as S,r as c,i as u,w as n,E as B,x as M,p as $}from"./index-c6bd05ee.js";const I={class:"date-status"},A=V({__name:"ResourceDateStatus",props:{creationTime:{},modificationTime:{}},setup(p){const{t:a,formatIsoDate:d}=g(),l=p,m=w(()=>d(l.creationTime)),s=w(()=>d(l.modificationTime));return(f,h)=>(t(),k("span",I,[r(i(_(a)("common.detail.created"))+": "+i(m.value)+" ",1),e(_(R)),r(" "+i(_(a)("common.detail.modified"))+": "+i(s.value),1)]))}});const C=x(A,[["__scopeId","data-v-fa366713"]]),N={key:2,class:"stack","data-testid":"detail-view-details"},b={class:"date-status-wrapper"},E=V({__name:"MeshDetailView",setup(p){const a=S();return(d,l)=>{const m=c("RouteTitle"),s=c("DataSource"),f=c("AppView"),h=c("RouteView");return t(),u(h,{name:"mesh-detail-view",params:{mesh:""}},{default:n(({route:v,t:T})=>[e(m,{title:T("meshes.routes.overview.title")},null,8,["title"]),r(),e(f,null,{default:n(()=>[e(s,{src:`/meshes/${v.params.mesh}`},{default:n(({data:o,error:D})=>[e(s,{src:`/mesh-insights/${v.params.mesh}`},{default:n(({data:y})=>[D?(t(),u(B,{key:0,error:D},null,8,["error"])):o===void 0?(t(),u(M,{key:1})):(t(),k("div",N,[e(_(a),{mesh:o,"mesh-insight":y},null,8,["mesh","mesh-insight"]),r(),$("div",b,[e(C,{"creation-time":o.creationTime,"modification-time":o.modificationTime},null,8,["creation-time","modification-time"])])]))]),_:2},1032,["src"])]),_:2},1032,["src"])]),_:2},1024)]),_:1})}}});const H=x(E,[["__scopeId","data-v-fab0c2e7"]]);export{H as default};
