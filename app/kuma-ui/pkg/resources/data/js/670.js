"use strict";(self["webpackChunkkuma_gui"]=self["webpackChunkkuma_gui"]||[]).push([[670],{56882:function(t,a,e){e.d(a,{Z:function(){return w}});var s=function(){var t=this,a=t._self._c;return a("div",{staticClass:"data-overview",attrs:{"data-testid":"data-overview"}},[a("div",{staticClass:"data-table-controls mb-2"},[t._t("additionalControls"),a("KButton",{staticClass:"ml-2 refresh-button",attrs:{appearance:"primary",size:"small",disabled:t.isLoading},on:{click:t.onRefreshButtonClick}},[a("div",{staticClass:"refresh-icon",class:{"is-spinning":t.isLoading}},[a("svg",{attrs:{xmlns:"http://www.w3.org/2000/svg",viewBox:"0 0 36 36"}},[a("g",{attrs:{fill:"#fff","fill-rule":"nonzero"}},[a("path",{attrs:{d:"M18 5.5a12.465 12.465 0 00-8.118 2.995 1.5 1.5 0 001.847 2.363l.115-.095A9.437 9.437 0 0118 8.5l.272.004a9.487 9.487 0 019.07 7.75l.04.246H25a.5.5 0 00-.416.777l4 6a.5.5 0 00.832 0l4-6 .04-.072A.5.5 0 0033 16.5h-2.601l-.017-.15C29.567 10.2 24.294 5.5 18 5.5zM2.584 18.723l-.04.072A.5.5 0 003 19.5h2.6l.018.15C6.433 25.8 11.706 30.5 18 30.5c3.013 0 5.873-1.076 8.118-2.995a1.5 1.5 0 00-1.847-2.363l-.115.095A9.437 9.437 0 0118 27.5l-.272-.004a9.487 9.487 0 01-9.07-7.75l-.041-.246H11a.5.5 0 00.416-.777l-4-6a.5.5 0 00-.832 0l-4 6z"}})])])]),a("span",[t._v("Refresh")])])],2),t.isReady?a("div",{staticClass:"data-overview-content"},[!t.tableDataIsEmpty&&t.tableData?a("div",{staticClass:"data-overview-table"},[a("KTable",{staticClass:"micro-table",class:{"data-table-is-hidden":t.tableDataIsEmpty,"has-border":t.tableHasBorder},attrs:{options:t.tableDataFiltered,"has-side-border":!1,"has-hover":"","is-clickable":""},on:{"row:click":t.tableRowHandler},scopedSlots:t._u([t._l(t.customSlots,(function(a){return{key:a,fn:function({rowValue:e,row:s}){return[t._t(a,null,{rowValue:e,row:s})]}}})),{key:"status",fn:function({rowValue:e}){return[a("div",{staticClass:"entity-status",class:{"is-offline":"offline"===e.toString().toLowerCase()||!1===e,"is-degraded":"partially degraded"===e.toString().toLowerCase()||!1===e}},[a("span",{staticClass:"entity-status__dot"}),a("span",{staticClass:"entity-status__label"},[t._v(t._s(e))])])]}},{key:"tags",fn:function({rowValue:e}){return t._l(e,(function(t,e){return a("EntityTag",{key:e,attrs:{tag:t}})}))}},{key:"totalUpdates",fn:function({row:e}){return[a("span",{staticClass:"entity-total-updates"},[a("span",[t._v(" "+t._s(e.totalUpdates)+" ")])])]}},{key:"actions",fn:function({row:e}){return[a("a",{staticClass:"data-table-action-link",class:{"is-active":t.selectedRow===e.name}},[t.selectedRow===e.name?a("span",{staticClass:"action-link__active-state"},[t._v(" ✓ "),a("span",{staticClass:"sr-only"},[t._v(" Selected ")])]):a("span",{staticClass:"action-link__normal-state"},[t._v(" View ")])])]}},{key:"dpVersion",fn:function({row:e,rowValue:s}){return[a("div",{class:{"with-warnings":e.unsupportedEnvoyVersion||e.unsupportedKumaDPVersion||e.kumaDpAndKumaCpMismatch}},[t._v(" "+t._s(s)+" ")])]}},{key:"envoyVersion",fn:function({row:e,rowValue:s}){return[a("div",{class:{"with-warnings":e.unsupportedEnvoyVersion}},[t._v(" "+t._s(s)+" ")])]}},t.showWarnings?{key:"warnings",fn:function({row:t}){return[t.withWarnings?a("KIcon",{staticClass:"mr-1",attrs:{color:"var(--yellow-400)",icon:"warning","secondary-color":"var(--black-75)",size:"20"}}):a("div")]}}:null],null,!0)}),a("PaginationWidget",{attrs:{"has-previous":t.pageOffset>0,"has-next":t.next},on:{next:t.goToNextPage,previous:t.goToPreviousPage}})],1):t._e(),t.tableDataIsEmpty&&t.tableData?a("KEmptyState",{attrs:{"cta-is-hidden":""},scopedSlots:t._u([{key:"title",fn:function(){return[a("div",{staticClass:"card-icon mb-3"},[a("img",{attrs:{src:e(34727)}})]),t.emptyState.title?a("span",[t._v(" "+t._s(t.emptyState.title)+" ")]):a("span",[t._v(" No Items Found ")])]},proxy:!0},t.emptyState.message?{key:"message",fn:function(){return[t._v(" "+t._s(t.emptyState.message)+" ")]},proxy:!0}:null],null,!0)}):t._e(),t.$slots.content?a("div",{staticClass:"data-overview-content mt-6"},[t._t("content")],2):t._e()],1):t._e(),t.isLoading?a("KEmptyState",{attrs:{"cta-is-hidden":""},scopedSlots:t._u([{key:"title",fn:function(){return[a("div",{staticClass:"card-icon mb-3"},[a("KIcon",{attrs:{icon:"spinner",color:"rgba(0, 0, 0, 0.25)",size:"42"}})],1),t._v(" Data Loading… ")]},proxy:!0}],null,!1,4031249790)}):t._e(),t.hasError?a("KEmptyState",{attrs:{"cta-is-hidden":""},scopedSlots:t._u([{key:"title",fn:function(){return[a("div",{staticClass:"card-icon mb-3"},[a("KIcon",{staticClass:"kong-icon--centered",attrs:{color:"var(--yellow-200)",icon:"warning","secondary-color":"var(--black-75)",size:"42"}})],1),t._v(" An error has occurred while trying to load this data. ")]},proxy:!0}],null,!1,822917942)}):t._e()],1)},n=[],i=e(89340),o=e(70878),r=function(){var t=this,a=t._self._c;return a("div",{staticClass:"pagination"},[t.hasPrevious?a("KButton",{ref:"paginatePrev",attrs:{appearance:"primary"},on:{click:t.onPreviousButtonClick}},[t._v(" ‹ Previous ")]):t._e(),t.hasNext?a("KButton",{ref:"paginateNext",attrs:{appearance:"primary"},on:{click:t.onNextButtonClick}},[t._v(" Next › ")]):t._e()],1)},l=[],c={name:"PaginationWidget",props:{hasPrevious:{type:Boolean,default:!1},hasNext:{type:Boolean,default:!1}},methods:{onNextButtonClick(){this.$emit("next"),i.fy.logger.info(o.T.PAGINATION_NEXT_BUTTON_CLICKED)},onPreviousButtonClick(){this.$emit("previous"),i.fy.logger.info(o.T.PAGINATION_PREVIOUS_BUTTON_CLICKED)}}},d=c,u=e(1001),g=(0,u.Z)(d,r,l,!1,null,"c8e98246",null),p=g.exports,y=function(){var t=this,a=t._self._c;return a("span",{staticClass:"entity-tag"},[a("span",{staticClass:"entity-tag__label",class:`entity-tag__label--${t.cleanTagLabel(t.tag.label)}`},[t._v(" "+t._s(t.tag.label)+" ")]),a("span",{staticClass:"entity-tag__value",class:`entity-tag__value--${t.tag.value}`},[t._v(" "+t._s(t.tag.value)+" ")])])},h=[],f={name:"EntityTag",props:{tag:{type:Object,required:!0}},methods:{cleanTagLabel(t){return t.toLowerCase().replace(".","-").replace("/","-")}}},m=f,_=(0,u.Z)(m,y,h,!1,null,"287dac0d",null),b=_.exports,v={name:"DataOverview",components:{PaginationWidget:p,EntityTag:b},props:{pageSize:{type:Number,default:12},isLoading:{type:Boolean,default:!1},hasError:{type:Boolean,default:!1},isEmpty:{type:Boolean,default:!1},emptyState:{type:Object,default:null},tableData:{type:Object,default:null},tableHasBorder:{type:Boolean,required:!1,default:!1},tableDataIsEmpty:{type:Boolean,default:!1},showWarnings:{type:Boolean},next:{type:Boolean,default:!1}},data(){return{selectedRow:"",pageOffset:0}},computed:{customSlots(){return this.tableData.headers.map((({key:t})=>t)).filter((t=>this.$scopedSlots[t]))},isReady(){return!this.isEmpty&&!this.hasError&&!this.isLoading},tableDataFiltered(){const t=this.tableData.data,a=this.tableData.headers,e={headers:a,data:[...t]};return this.showWarnings||(e.headers=e.headers.filter((({key:t})=>"warnings"!==t))),e}},watch:{isLoading(t){!t&&this.tableData.data.length>0&&(this.selectedRow=this.tableData.data[0].name)}},methods:{tableRowHandler(t,a,e){this.selectedRow=a.name,this.$emit("tableAction",a)},onRefreshButtonClick(){this.$emit("refresh"),this.$emit("loadData",this.pageOffset),i.fy.logger.info(o.T.TABLE_REFRESH_BUTTON_CLICKED)},goToPreviousPage(){this.pageOffset-=this.pageSize,this.$emit("loadData",this.pageOffset)},goToNextPage(){this.pageOffset+=this.pageSize,this.$emit("loadData",this.pageOffset)}}},T=v,C=(0,u.Z)(T,s,n,!1,null,null,null),w=C.exports},33561:function(t,a,e){e.d(a,{Z:function(){return c}});var s=function(){var t=this,a=t._self._c;return a("div",[t.isReady?a("div",{staticClass:"label-list-content"},[t.isLoading||t.isEmpty?t._e():a("KCard",{attrs:{"border-variant":"noBorder"},scopedSlots:t._u([{key:"body",fn:function(){return[a("div",{staticClass:"label-list__col-wrapper multi-col"},[t._t("default")],2)]},proxy:!0}],null,!0)})],1):t._e(),t.isLoading?a("KEmptyState",{attrs:{"cta-is-hidden":""},scopedSlots:t._u([{key:"title",fn:function(){return[a("div",{staticClass:"card-icon mb-3"},[a("KIcon",{attrs:{icon:"spinner",color:"rgba(0, 0, 0, 0.1)",size:"42"}})],1),t._v(" Data Loading... ")]},proxy:!0}],null,!1,3263214496)}):t._e(),t.isEmpty&&!t.isLoading?a("KEmptyState",{attrs:{"cta-is-hidden":""},scopedSlots:t._u([{key:"title",fn:function(){return[a("div",{staticClass:"card-icon mb-3"},[a("KIcon",{staticClass:"kong-icon--centered",attrs:{color:"var(--yellow-200)",icon:"warning","secondary-color":"var(--black-75)",size:"42"}})],1),t._v(" There is no data to display. ")]},proxy:!0}],null,!1,1612658095)}):t._e(),t.hasError?a("KEmptyState",{attrs:{"cta-is-hidden":""},scopedSlots:t._u([{key:"title",fn:function(){return[a("div",{staticClass:"card-icon mb-3"},[a("KIcon",{staticClass:"kong-icon--centered",attrs:{color:"var(--yellow-200)",icon:"warning","secondary-color":"var(--black-75)",size:"42"}})],1),t._v(" An error has occurred while trying to load this data. ")]},proxy:!0}],null,!1,822917942)}):t._e()],1)},n=[],i={name:"LabelList",props:{items:{type:Object,default:null},title:{type:String,default:null},isLoading:{type:Boolean,default:!1},hasError:{type:Boolean,default:!1},isEmpty:{type:Boolean,default:!1}},computed:{isReady(){return!this.isEmpty&&!this.hasError&&!this.isLoading}}},o=i,r=e(1001),l=(0,r.Z)(o,s,n,!1,null,null,null),c=l.exports},7001:function(t,a,e){e.d(a,{Z:function(){return p}});var s=function(){var t=this,a=t._self._c;return a("div",{staticClass:"tab-container",attrs:{"data-testid":"tab-container"}},[t.$slots.tabHeader&&t.isReady?a("header",{staticClass:"tab__header"},[t._t("tabHeader")],2):t._e(),a("div",{staticClass:"tab__content-container"},[t.isReady?a("KTabs",{attrs:{tabs:t.tabs},on:{changed:a=>t.switchTab(a)},scopedSlots:t._u([t._l(t.tabsSlots,(function(a){return{key:a,fn:function(){return[t._t(a)]},proxy:!0}})),{key:"warnings-anchor",fn:function(){return[a("span",{staticClass:"flex items-center with-warnings"},[a("KIcon",{staticClass:"mr-1",attrs:{color:"var(--yellow-400)",icon:"warning","secondary-color":"var(--black-75)",size:"16"}}),a("span",[t._v(" Warnings ")])],1)]},proxy:!0}],null,!0),model:{value:t.tabState,callback:function(a){t.tabState=a},expression:"tabState"}}):t._e(),!0===t.loaders?a("div",[t.isLoading?a("KEmptyState",{attrs:{"cta-is-hidden":""},scopedSlots:t._u([{key:"title",fn:function(){return[a("div",{staticClass:"card-icon mb-3"},[a("KIcon",{attrs:{icon:"spinner",color:"rgba(0, 0, 0, 0.1)",size:"42"}})],1),t._v(" Data Loading... ")]},proxy:!0}],null,!1,3263214496)}):t._e(),t.hasError?a("KEmptyState",{attrs:{"cta-is-hidden":""},scopedSlots:t._u([{key:"title",fn:function(){return[a("div",{staticClass:"card-icon mb-3"},[a("KIcon",{staticClass:"kong-icon--centered",attrs:{color:"var(--yellow-200)",icon:"warning","secondary-color":"var(--black-75)",size:"42"}})],1),t._v(" An error has occurred while trying to load this data. ")]},proxy:!0}],null,!1,822917942)}):t._e()],1):t._e()],1)])},n=[],i=e(30037),o=e.n(i),r=e(89340),l=e(70878),c={name:"TabsWidget",components:{KEmptyState:o()},props:{loaders:{type:Boolean,default:!0},isLoading:{type:Boolean,default:!1},isEmpty:{type:Boolean,default:!1},hasError:{type:Boolean,default:!1},tabs:{type:Array,required:!0},hasBorder:{type:Boolean,default:!1},initialTabOverride:{type:String,default:null}},data(){return{tabState:this.initialTabOverride&&`#${this.initialTabOverride}`}},computed:{tabsSlots(){return this.tabs.map((t=>t.hash.replace("#","")))},isReady(){return!1===this.loaders||!this.isEmpty&&!this.hasError&&!this.isLoading}},methods:{switchTab(t){r.fy.logger.info(l.T.TABS_TAB_CHANGE,{data:{newTab:t}}),this.$emit("onTabChange",t)}}},d=c,u=e(1001),g=(0,u.Z)(d,s,n,!1,null,"cbdcede8",null),p=g.exports},70878:function(t,a,e){e.d(a,{T:function(){return s}});const s={PAGINATION_PREVIOUS_BUTTON_CLICKED:"pagination-previous-button-clicked",PAGINATION_NEXT_BUTTON_CLICKED:"pagination-next-button-clicked",SIDEBAR_ITEM_CLICKED:"sidebar-item-clicked",TABLE_REFRESH_BUTTON_CLICKED:"table-refresh-button-clicked",TABS_TAB_CHANGE:"tabs-tab-change",CREATE_MESH_CLICKED:"create-mesh-clicked",CREATE_DATA_PLANE_PROXY_CLICKED:"create-data-plane-proxy-clicked"}},70172:function(t,a,e){e.d(a,{W:function(){return r}});var s=e(45689);function n(t){return[...t].sort(((t,a)=>t.name>a.name||t.name===a.name&&t.mesh>a.mesh?1:-1))}const i=t=>0!==t.total&&t.items&&t.items.length>0?n(t.items):[];function o({getSingleEntity:t,getAllEntities:a,getAllEntitiesFromPath:e,getAllEntitiesFromMesh:s,path:n,mesh:i,query:o,size:r,offset:l,params:c={}}){const d={size:r,offset:l,...c};if(t&&o)return t({mesh:i,path:n,name:o},d);const u=!i||"all"===i;return a&&u?a(d):e&&u?e({path:n},d):s&&i?s({mesh:i,path:n},d):Promise.resolve()}async function r({getSingleEntity:t,getAllEntities:a,getAllEntitiesFromPath:e,getAllEntitiesFromMesh:n,path:r,mesh:l,query:c,size:d=s.NR,offset:u,params:g={}}){const p=await o({getSingleEntity:t,getAllEntities:a,getAllEntitiesFromPath:e,getAllEntitiesFromMesh:n,path:r,mesh:l,query:c,size:d,offset:u,params:g});return p?{data:p.items?i(p):[p],next:Boolean(p.next)}:{data:[],next:!1}}},34727:function(t){t.exports="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0OCIgaGVpZ2h0PSI0MiI+CiAgPHBhdGggZmlsbD0iI0Q5RDlEOSIgZmlsbC1ydWxlPSJldmVub2RkIiBkPSJNNDggNHY1aC0yVjZIMnYzSDBWMkMwIC44OTU0MzA1Ljg5NTQzMSAwIDIgMGg0NGMxLjEwNDU2OSAwIDIgLjg5NTQzMDUgMiAydjJ6bS0yIDI2aC00di0yaDR2LTNoMnY4aC0ydi0zek0yIDMwdjNIMHYtOGgydjNoNHYySDJ6bTQ0LTEyaC00di0yaDR2LTNoMnY4aC0ydi0zek0yIDE4djNIMHYtOGgydjNoNHYySDJ6bTgtMmg0djJoLTR2LTJ6bTggMGg0djJoLTR2LTJ6bTggMGg0djJoLTR2LTJ6bTggMGg0djJoLTR2LTJ6TTEwIDI4aDR2MmgtNHYtMnptOCAwaDR2MmgtNHYtMnptOCAwaDR2MmgtNHYtMnptOCAwaDR2MmgtNHYtMnptMTIgMTRoLTR2LTJoNHYtM2gydjNjMCAxLjEwNDU2OTUtLjg5NTQzMSAyLTIgMnpNMiA0MGg0djJIMmMtMS4xMDQ1NjkgMC0yLS44OTU0MzA1LTItMnYtM2gydjN6bTggMGg0djJoLTR2LTJ6bTggMGg0djJoLTR2LTJ6bTggMGg0djJoLTR2LTJ6bTggMGg0djJoLTR2LTJ6TTIgMnYyaDQ0VjJIMnoiLz4KPC9zdmc+Cg=="}}]);