(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-2d0f0fc2"],{"9f65":function(t,e,n){"use strict";n.r(e);var o=function(){var t=this,e=t.$createElement,n=t._self._c||e;return n("div",{staticClass:"dataplanes-detail"},[n("YamlView",{attrs:{title:"Entity Overview","has-error":t.hasError,"is-loading":t.isLoading,"is-empty":t.isEmpty,content:t.content}})],1)},a=[],i=n("ff9d"),s={name:"HealthChecksDetail",metaInfo:{title:"Health Check Details"},components:{YamlView:i["a"]},data:function(){return{content:null,hasError:!1,isLoading:!0,isEmpty:!1}},watch:{$route:function(t,e){this.bootstrap()}},beforeMount:function(){this.bootstrap()},methods:{bootstrap:function(){var t=this,e=this.$route.params.mesh,n=this.$route.params.healthcheck;return this.$api.getHealthCheckFromMesh(e,n).then((function(e){e?t.content=e:t.$router.push("/404")})).catch((function(e){t.hasError=!0,console.error(e)})).finally((function(){setTimeout((function(){t.isLoading=!1}),"500")}))}}},r=s,c=n("2877"),h=Object(c["a"])(r,o,a,!1,null,null,null);e["default"]=h.exports}}]);