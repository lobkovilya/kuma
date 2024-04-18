// Package types provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen version v1.15.0 DO NOT EDIT.
package types

import (
	"time"

	externalRef0 "github.com/kumahq/kuma/api/openapi/types/common"
)

// Defines values for GetMeshesMeshDataplanesNameConfigParamsInclude.
const (
	Diff GetMeshesMeshDataplanesNameConfigParamsInclude = "diff"
)

// Defines values for InspectDataplanesRulesParamsResourceType.
const (
	Dataplanes   InspectDataplanesRulesParamsResourceType = "dataplanes"
	Meshgateways InspectDataplanesRulesParamsResourceType = "meshgateways"
)

// BaseStatus defines model for BaseStatus.
type BaseStatus struct {
	Online int `json:"online"`
	Total  int `json:"total"`
}

// DataplanesStats Dataplanes statistics
type DataplanesStats struct {
	// GatewayBuiltin Builtin Gateway dataplane proxy statistics
	GatewayBuiltin FullStatus `json:"gatewayBuiltin"`

	// GatewayDelegated Delegated Gateway dataplane proxy statistics
	GatewayDelegated FullStatus `json:"gatewayDelegated"`

	// Standard Standard dataplane proxy statistics
	Standard FullStatus `json:"standard"`
}

// FullStatus defines model for FullStatus.
type FullStatus struct {
	Offline           int `json:"offline"`
	Online            int `json:"online"`
	PartiallyDegraded int `json:"partiallyDegraded"`
	Total             int `json:"total"`
}

// GlobalInsight Global Insight contains statistics for all main resources
type GlobalInsight struct {
	// CreatedAt Time of Global Insight creation
	CreatedAt time.Time `json:"createdAt"`

	// Dataplanes Dataplane proxy statistics
	Dataplanes DataplanesStats `json:"dataplanes"`

	// Meshes Mesh statistics
	Meshes MeshesStats `json:"meshes"`

	// Policies Policies statistics
	Policies PoliciesStats `json:"policies"`

	// Services Mesh services statistics
	Services ServicesStats `json:"services"`

	// Zones Zones statistics
	Zones ZonesStats `json:"zones"`
}

// InspectDataplanesConfig defines model for InspectDataplanesConfig.
type InspectDataplanesConfig struct {
	// Diff Contains a diff in a JSONPatch format between the XDS config returned in 'xds' and the current proxy XDS config.
	// By default, the field is empty. To include the diff in the response, use the `include=diff` query parameter.
	Diff *[]externalRef0.JsonPatchItem `json:"diff,omitempty"`

	// Xds The raw XDS config as an inline JSON object
	Xds map[string]interface{} `json:"xds"`
}

// InspectDataplanesForPolicy A list of proxies
type InspectDataplanesForPolicy struct {
	Items []externalRef0.Meta `json:"items"`
	Next  *string             `json:"next,omitempty"`
	Total int                 `json:"total"`
}

// InspectRules A list of rules for a dataplane
type InspectRules struct {
	HttpMatches []externalRef0.HttpMatch   `json:"httpMatches"`
	Resource    externalRef0.Meta          `json:"resource"`
	Rules       []externalRef0.InspectRule `json:"rules"`
}

// MeshesStats Mesh statistics
type MeshesStats struct {
	// Total Number of meshes
	Total int `json:"total"`
}

// PoliciesStats Policies statistics
type PoliciesStats struct {
	// Total Number of policies
	Total int `json:"total"`
}

// ResourceTypeDescriptionList A list of all resources install
type ResourceTypeDescriptionList struct {
	Resources []externalRef0.ResourceTypeDescription `json:"resources"`
}

// ServicesStats Services statistics
type ServicesStats struct {
	// External External services statistics
	External struct {
		Total int `json:"total"`
	} `json:"external"`

	// GatewayBuiltin Builtin Gateway services statistics
	GatewayBuiltin FullStatus `json:"gatewayBuiltin"`

	// GatewayDelegated Delegated Gateway services statistics
	GatewayDelegated FullStatus `json:"gatewayDelegated"`

	// Internal Internal services statistics
	Internal FullStatus `json:"internal"`
}

// ZonesStats Zone statistics
type ZonesStats struct {
	// ControlPlanes Control Planes statistics
	ControlPlanes BaseStatus `json:"controlPlanes"`

	// ZoneEgresses Zone Egresses statistics
	ZoneEgresses BaseStatus `json:"zoneEgresses"`

	// ZoneIngresses Zone Ingresses statistics
	ZoneIngresses BaseStatus `json:"zoneIngresses"`
}

// BadRequest standard error
type BadRequest = externalRef0.Error

// GlobalInsightResponse Global Insight contains statistics for all main resources
type GlobalInsightResponse = GlobalInsight

// InspectDataplanesConfigResponse defines model for InspectDataplanesConfigResponse.
type InspectDataplanesConfigResponse = InspectDataplanesConfig

// InspectDataplanesForPolicyResponse A list of proxies
type InspectDataplanesForPolicyResponse = InspectDataplanesForPolicy

// InspectRulesResponse A list of rules for a dataplane
type InspectRulesResponse = InspectRules

// InternalServerError standard error
type InternalServerError = externalRef0.Error

// ResourceTypeDescriptionListResponse A list of all resources install
type ResourceTypeDescriptionListResponse = ResourceTypeDescriptionList

// GetMeshesMeshDataplanesNameConfigParams defines parameters for GetMeshesMeshDataplanesNameConfig.
type GetMeshesMeshDataplanesNameConfigParams struct {
	// Shadow When computing XDS config the CP takes into account policies with 'kuma.io/effect: shadow' label
	Shadow *bool `form:"shadow,omitempty" json:"shadow,omitempty"`

	// Include An array of extra fields to include in the response. When `include=diff` the server computes a diff in JSONPatch format
	// between the current proxy XDS config and the config returned in the 'xds' field.
	Include *[]GetMeshesMeshDataplanesNameConfigParamsInclude `form:"include,omitempty" json:"include,omitempty"`
}

// GetMeshesMeshDataplanesNameConfigParamsInclude defines parameters for GetMeshesMeshDataplanesNameConfig.
type GetMeshesMeshDataplanesNameConfigParamsInclude string

// InspectResourcesParams defines parameters for InspectResources.
type InspectResourcesParams struct {
	// Size The max number of items to return
	Size *int `form:"size,omitempty" json:"size,omitempty"`

	// Offset The offset of result
	Offset *int `form:"offset,omitempty" json:"offset,omitempty"`

	// Name A sub string to filter resources by name
	Name *string `form:"name,omitempty" json:"name,omitempty"`
}

// InspectDataplanesRulesParamsResourceType defines parameters for InspectDataplanesRules.
type InspectDataplanesRulesParamsResourceType string
