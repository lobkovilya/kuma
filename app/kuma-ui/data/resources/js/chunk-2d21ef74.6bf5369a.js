(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-2d21ef74"],{d88b:function(t,e,i){"use strict";i.r(e);var n=function(){var t=this,e=t.$createElement,i=t._self._c||e;return i("div",{staticClass:"dataplanes-detail"},[i("YamlView",{attrs:{title:"Entity Overview",content:t.entity}})],1)},a=[],r=i("ff9d"),s=i("be10"),o={name:"TrafficPermissionsDetail",metaInfo:{title:"Traffic Permission Details"},components:{MetricGrid:s["a"],YamlView:r["a"]},data:function(){return{entity:null}},watch:{$route:function(t,e){this.bootstrap()}},beforeMount:function(){this.bootstrap()},methods:{bootstrap:function(){var t=this,e=this.$route.params.mesh,i=this.$route.params.trafficpermission;return this.$api.getTrafficPermission(e,i).then((function(e){e?t.entity=e:t.$router.push("/404")})).catch((function(e){console.error(e),t.entity=e}))}}},c=o,u=i("2877"),f=Object(u["a"])(c,n,a,!1,null,null,null);e["default"]=f.exports}}]);