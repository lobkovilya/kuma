(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-5c1983ae"],{2055:function(e,t,r){},"43c3":function(e,t,r){"use strict";var a=function(){var e=this,t=e.$createElement,r=e._self._c||t;return r("header",{staticClass:"page-header",class:{"flex justify-between items-center my-6":!e.noflex}},[e._t("default")],2)},n=[],c={props:{noflex:{type:Boolean,default:!1}}},u=c,i=(r("e234"),r("2877")),s=Object(i["a"])(u,a,n,!1,null,null,null);t["a"]=s.exports},"5aa0":function(e,t,r){"use strict";var a=r("70e0"),n=r.n(a);n.a},"6f00":function(e,t,r){"use strict";var a=function(){var e=this,t=e.$createElement,r=e._self._c||t;return r("section",{staticClass:"page-content"},[e._t("default")],2)},n=[],c={},u=c,i=(r("5aa0"),r("2877")),s=Object(i["a"])(u,a,n,!1,null,null,null);t["a"]=s.exports},"70e0":function(e,t,r){},af21:function(e,t,r){},deb3:function(e,t,r){"use strict";r.r(t);var a=function(){var e=this,t=e.$createElement,r=e._self._c||t;return r("div",{staticClass:"overview"},[r("page-header",{attrs:{noflex:""}},[r("breadcrumbs"),r("h2",{staticClass:"xxl"},[e._v("\n      "+e._s(e.pageTitle)+"\n    ")])],1),r("page-content",[r("transition",{attrs:{mode:"out-in",name:"fade"}},[r("router-view")],1)],1)],1)},n=[],c=r("43c3"),u=r("6f00"),i=function(){var e=this,t=e.$createElement,r=e._self._c||t;return r("div",[e.hideBreadcrumbs?e._e():r("Krumbs",{attrs:{items:e.routes}})],1)},s=[],o=(r("8e6e"),r("456d"),r("ac6a"),r("8615"),r("bd86")),m=(r("28a5"),r("7f7f"),r("d7c2"));function l(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function p(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?l(Object(r),!0).forEach((function(t){Object(o["a"])(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):l(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}var f={computed:{pageMesh:function(){return this.$route.params.mesh},routes:function(){var e=this,t=[];this.$route.matched.map((function(r){e.calculateRouteText(r),e.calculateRouteTitle(r);var a=void 0!==r.redirect&&void 0!==r.redirect.name?r.redirect.name:r.name;e.isCurrentRoute(r)&&e.pageMesh&&"mesh-overview"!==e.$route.name&&t.push({key:e.pageMesh,to:{name:"mesh-overview"},title:"Mesh Overview for ".concat(e.pageMesh),text:e.pageMesh}),e.isCurrentRoute(r)&&r.meta.parent&&"undefined"!==r.meta.parent?t.push({key:r.meta.parent,to:{name:r.meta.parent},title:r.meta.title,text:r.meta.breadcrumb||r.meta.title}):e.isCurrentRoute(r)&&!r.meta.excludeAsBreadcrumb?t.push({key:a,to:{name:a},title:r.meta.title,text:r.meta.breadcrumb||r.meta.title}):r.meta.parent&&"undefined"!==r.meta.parent&&t.push({key:r.meta.parent,to:{name:r.meta.parent},title:r.meta.title,text:r.meta.breadcrumb||r.meta.title})}));var r=this.calculateRouteTextAdvanced(this.$route);return r&&t.push({title:r,text:r}),t},hideBreadcrumbs:function(){return this.$route.query.hide_breadcrumb}},methods:{getBreadcrumbItem:function(e,t,r,a){return{key:e,to:t,title:r,text:a}},isCurrentRoute:function(e){return e.name&&e.name===this.$router.currentRoute.name||e.redirect===this.$router.currentRoute.name},calculateRouteFromQuery:function(e){var t=e.entity_id,r=e.entity_type;if(t&&r){var a=this.$router.resolve({name:"show-".concat(r.split("_")[0]),params:{id:t.split(",")[0]}}).normalizedTo,n=p({},a,{meta:p({},a.meta)}),c=n.params.id.split("-")[0];return t.split(",").length>1&&t.split(",")[1]&&(c=t.split(",")[1]),n.meta.breadcrumb=c,[p({},this.getBreadcrumbItem(n.name,n,this.calculateRouteTitle(n),this.calculateRouteText(n)))]}},calculateRouteText:function(e){if(e.path&&e.path.indexOf(":mesh")>-1){var t=this.$router.currentRoute.params;return(t&&t.mesh&&Object(m["b"])(t.mesh)?t.mesh.split("-")[0].trim():t.mesh)||e.meta.breadcrumb||e.meta.title}return e.meta&&(e.meta.breadcrumb||e.meta.title)||e.name||e.meta.breadcrumb||e.meta.title},calculateRouteTitle:function(e){return e.params&&e.params.mesh||e.path.indexOf(":mesh")>-1&&this.$router.currentRoute.params&&this.$router.currentRoute.params.mesh},calculateRouteTextAdvanced:function(e){var t=e.params,r="mesh-overview"===e.name,a=Object.assign({},t,{mesh:null});return r?t.mesh:Object.values(a).filter((function(e){return e}))[0]}}},h=f,d=(r("e7ab"),r("2877")),b=Object(d["a"])(h,i,s,!1,null,null,null),v=b.exports,O={name:"Shell",components:{PageHeader:c["a"],PageContent:u["a"],Breadcrumbs:v},computed:{pageTitle:function(){var e,t=this.$route.meta.title,r=this.$route.params.mesh,a=this.$route.params.dataplane,n=this.$route.params.trafficpermission,c=this.$route.params.trafficlog,u=this.$route.params.trafficroute,i=this.$route.params.healthcheck;return e=a?"".concat(t," for ").concat(a):c?"".concat(t," for ").concat(c):u?"".concat(t," for ").concat(u):n?"".concat(t," for ").concat(n):i?"".concat(t," for ").concat(i):r?"".concat(t," for ").concat(r):t,e}}},g=O,x=Object(d["a"])(g,a,n,!1,null,null,null);t["default"]=x.exports},e234:function(e,t,r){"use strict";var a=r("2055"),n=r.n(a);n.a},e7ab:function(e,t,r){"use strict";var a=r("af21"),n=r.n(a);n.a}}]);