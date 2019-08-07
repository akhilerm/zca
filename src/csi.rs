#![allow(clippy::type_complexity)]
#![allow(clippy::unit_arg)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::enum_variant_names)]
#![allow(dead_code)]
/// Intentionally empty
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPluginInfoRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPluginInfoResponse {
    /// The name MUST follow domain name notation format
    /// (https://tools.ietf.org/html/rfc1035#section-2.3.1). It SHOULD
    /// include the plugin's host company name and the plugin name,
    /// to minimize the possibility of collisions. It MUST be 63
    /// characters or less, beginning and ending with an alphanumeric
    /// character ([a-z0-9A-Z]) with dashes (-), dots (.), and
    /// alphanumerics between. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub name: std::string::String,
    /// This field is REQUIRED. Value of this field is opaque to the CO.
    #[prost(string, tag = "2")]
    pub vendor_version: std::string::String,
    /// This field is OPTIONAL. Values are opaque to the CO.
    #[prost(map = "string, string", tag = "3")]
    pub manifest: ::std::collections::HashMap<std::string::String, std::string::String>,
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPluginCapabilitiesRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPluginCapabilitiesResponse {
    /// All the capabilities that the controller service supports. This
    /// field is OPTIONAL.
    #[prost(message, repeated, tag = "1")]
    pub capabilities: ::std::vec::Vec<PluginCapability>,
}
/// Specifies a capability of the plugin.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PluginCapability {
    #[prost(oneof = "plugin_capability::Type", tags = "1, 2")]
    pub r#type: ::std::option::Option<plugin_capability::Type>,
}
pub mod plugin_capability {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Service {
        #[prost(enumeration = "service::Type", tag = "1")]
        pub r#type: i32,
    }
    pub mod service {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Type {
            Unknown = 0,
            /// CONTROLLER_SERVICE indicates that the Plugin provides RPCs for
            /// the ControllerService. Plugins SHOULD provide this capability.
            /// In rare cases certain plugins MAY wish to omit the
            /// ControllerService entirely from their implementation, but such
            /// SHOULD NOT be the common case.
            /// The presence of this capability determines whether the CO will
            /// attempt to invoke the REQUIRED ControllerService RPCs, as well
            /// as specific RPCs as indicated by ControllerGetCapabilities.
            ControllerService = 1,
            /// VOLUME_ACCESSIBILITY_CONSTRAINTS indicates that the volumes for
            /// this plugin MAY NOT be equally accessible by all nodes in the
            /// cluster. The CO MUST use the topology information returned by
            /// CreateVolumeRequest along with the topology information
            /// returned by NodeGetInfo to ensure that a given volume is
            /// accessible from a given node when scheduling workloads.
            VolumeAccessibilityConstraints = 2,
        }
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct VolumeExpansion {}
    pub mod volume_expansion {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Type {
            Unknown = 0,
            /// ONLINE indicates that volumes may be expanded when published to
            /// a node. When a Plugin implements this capability it MUST
            /// implement either the EXPAND_VOLUME controller capability or the
            /// EXPAND_VOLUME node capability or both. When a plugin supports
            /// ONLINE volume expansion and also has the EXPAND_VOLUME
            /// controller capability then the plugin MUST support expansion of
            /// volumes currently published and available on a node. When a
            /// plugin supports ONLINE volume expansion and also has the
            /// EXPAND_VOLUME node capability then the plugin MAY support
            /// expansion of node-published volume via NodeExpandVolume.
            ///
            /// Example 1: Given a shared filesystem volume (e.g. GlusterFs),
            ///   the Plugin may set the ONLINE volume expansion capability and
            ///   implement ControllerExpandVolume but not NodeExpandVolume.
            ///
            /// Example 2: Given a block storage volume type (e.g. EBS), the
            ///   Plugin may set the ONLINE volume expansion capability and
            ///   implement both ControllerExpandVolume and NodeExpandVolume.
            ///
            /// Example 3: Given a Plugin that supports volume expansion only
            ///   upon a node, the Plugin may set the ONLINE volume
            ///   expansion capability and implement NodeExpandVolume but not
            ///   ControllerExpandVolume.
            Online = 1,
            /// OFFLINE indicates that volumes currently published and
            /// available on a node SHALL NOT be expanded via
            /// ControllerExpandVolume. When a plugin supports OFFLINE volume
            /// expansion it MUST implement either the EXPAND_VOLUME controller
            /// capability or both the EXPAND_VOLUME controller capability and
            /// the EXPAND_VOLUME node capability.
            ///
            /// Example 1: Given a block storage volume type (e.g. Azure Disk)
            ///   that does not support expansion of "node-attached" (i.e.
            ///   controller-published) volumes, the Plugin may indicate
            ///   OFFLINE volume expansion support and implement both
            ///   ControllerExpandVolume and NodeExpandVolume.
            Offline = 2,
        }
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        /// Service that the plugin supports.
        #[prost(message, tag = "1")]
        Service(Service),
        #[prost(message, tag = "2")]
        VolumeExpansion(VolumeExpansion),
    }
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProbeRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProbeResponse {
    /// Readiness allows a plugin to report its initialization status back
    /// to the CO. Initialization for some plugins MAY be time consuming
    /// and it is important for a CO to distinguish between the following
    /// cases:
    ///
    /// 1) The plugin is in an unhealthy state and MAY need restarting. In
    ///    this case a gRPC error code SHALL be returned.
    /// 2) The plugin is still initializing, but is otherwise perfectly
    ///    healthy. In this case a successful response SHALL be returned
    ///    with a readiness value of `false`. Calls to the plugin's
    ///    Controller and/or Node services MAY fail due to an incomplete
    ///    initialization state.
    /// 3) The plugin has finished initializing and is ready to service
    ///    calls to its Controller and/or Node services. A successful
    ///    response is returned with a readiness value of `true`.
    ///
    /// This field is OPTIONAL. If not present, the caller SHALL assume
    /// that the plugin is in a ready state and is accepting calls to its
    /// Controller and/or Node services (according to the plugin's reported
    /// capabilities).
    #[prost(message, optional, tag = "1")]
    pub ready: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateVolumeRequest {
    /// The suggested name for the storage space. This field is REQUIRED.
    /// It serves two purposes:
    /// 1) Idempotency - This name is generated by the CO to achieve
    ///    idempotency.  The Plugin SHOULD ensure that multiple
    ///    `CreateVolume` calls for the same name do not result in more
    ///    than one piece of storage provisioned corresponding to that
    ///    name. If a Plugin is unable to enforce idempotency, the CO's
    ///    error recovery logic could result in multiple (unused) volumes
    ///    being provisioned.
    ///    In the case of error, the CO MUST handle the gRPC error codes
    ///    per the recovery behavior defined in the "CreateVolume Errors"
    ///    section below.
    ///    The CO is responsible for cleaning up volumes it provisioned
    ///    that it no longer needs. If the CO is uncertain whether a volume
    ///    was provisioned or not when a `CreateVolume` call fails, the CO
    ///    MAY call `CreateVolume` again, with the same name, to ensure the
    ///    volume exists and to retrieve the volume's `volume_id` (unless
    ///    otherwise prohibited by "CreateVolume Errors").
    /// 2) Suggested name - Some storage systems allow callers to specify
    ///    an identifier by which to refer to the newly provisioned
    ///    storage. If a storage system supports this, it can optionally
    ///    use this name as the identifier for the new volume.
    /// Any Unicode string that conforms to the length limit is allowed
    /// except those containing the following banned characters:
    /// U+0000-U+0008, U+000B, U+000C, U+000E-U+001F, U+007F-U+009F.
    /// (These are control characters other than commonly used whitespace.)
    #[prost(string, tag = "1")]
    pub name: std::string::String,
    /// This field is OPTIONAL. This allows the CO to specify the capacity
    /// requirement of the volume to be provisioned. If not specified, the
    /// Plugin MAY choose an implementation-defined capacity range. If
    /// specified it MUST always be honored, even when creating volumes
    /// from a source; which MAY force some backends to internally extend
    /// the volume after creating it.
    #[prost(message, optional, tag = "2")]
    pub capacity_range: ::std::option::Option<CapacityRange>,
    /// The capabilities that the provisioned volume MUST have. SP MUST
    /// provision a volume that will satisfy ALL of the capabilities
    /// specified in this list. Otherwise SP MUST return the appropriate
    /// gRPC error code.
    /// The Plugin MUST assume that the CO MAY use the provisioned volume
    /// with ANY of the capabilities specified in this list.
    /// For example, a CO MAY specify two volume capabilities: one with
    /// access mode SINGLE_NODE_WRITER and another with access mode
    /// MULTI_NODE_READER_ONLY. In this case, the SP MUST verify that the
    /// provisioned volume can be used in either mode.
    /// This also enables the CO to do early validation: If ANY of the
    /// specified volume capabilities are not supported by the SP, the call
    /// MUST return the appropriate gRPC error code.
    /// This field is REQUIRED.
    #[prost(message, repeated, tag = "3")]
    pub volume_capabilities: ::std::vec::Vec<VolumeCapability>,
    /// Plugin specific parameters passed in as opaque key-value pairs.
    /// This field is OPTIONAL. The Plugin is responsible for parsing and
    /// validating these parameters. COs will treat these as opaque.
    #[prost(map = "string, string", tag = "4")]
    pub parameters: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// Secrets required by plugin to complete volume creation request.
    /// This field is OPTIONAL. Refer to the `Secrets Requirements`
    /// section on how to use this field.
    #[prost(map = "string, string", tag = "5")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// If specified, the new volume will be pre-populated with data from
    /// this source. This field is OPTIONAL.
    #[prost(message, optional, tag = "6")]
    pub volume_content_source: ::std::option::Option<VolumeContentSource>,
    /// Specifies where (regions, zones, racks, etc.) the provisioned
    /// volume MUST be accessible from.
    /// An SP SHALL advertise the requirements for topological
    /// accessibility information in documentation. COs SHALL only specify
    /// topological accessibility information supported by the SP.
    /// This field is OPTIONAL.
    /// This field SHALL NOT be specified unless the SP has the
    /// VOLUME_ACCESSIBILITY_CONSTRAINTS plugin capability.
    /// If this field is not specified and the SP has the
    /// VOLUME_ACCESSIBILITY_CONSTRAINTS plugin capability, the SP MAY
    /// choose where the provisioned volume is accessible from.
    #[prost(message, optional, tag = "7")]
    pub accessibility_requirements: ::std::option::Option<TopologyRequirement>,
}
/// Specifies what source the volume will be created from. One of the
/// type fields MUST be specified.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VolumeContentSource {
    #[prost(oneof = "volume_content_source::Type", tags = "1, 2")]
    pub r#type: ::std::option::Option<volume_content_source::Type>,
}
pub mod volume_content_source {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct SnapshotSource {
        /// Contains identity information for the existing source snapshot.
        /// This field is REQUIRED. Plugin is REQUIRED to support creating
        /// volume from snapshot if it supports the capability
        /// CREATE_DELETE_SNAPSHOT.
        #[prost(string, tag = "1")]
        pub snapshot_id: std::string::String,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct VolumeSource {
        /// Contains identity information for the existing source volume.
        /// This field is REQUIRED. Plugins reporting CLONE_VOLUME
        /// capability MUST support creating a volume from another volume.
        #[prost(string, tag = "1")]
        pub volume_id: std::string::String,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        #[prost(message, tag = "1")]
        Snapshot(SnapshotSource),
        #[prost(message, tag = "2")]
        Volume(VolumeSource),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateVolumeResponse {
    /// Contains all attributes of the newly created volume that are
    /// relevant to the CO along with information required by the Plugin
    /// to uniquely identify the volume. This field is REQUIRED.
    #[prost(message, optional, tag = "1")]
    pub volume: ::std::option::Option<Volume>,
}
/// Specify a capability of a volume.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VolumeCapability {
    /// This is a REQUIRED field.
    #[prost(message, optional, tag = "3")]
    pub access_mode: ::std::option::Option<volume_capability::AccessMode>,
    /// Specifies what API the volume will be accessed using. One of the
    /// following fields MUST be specified.
    #[prost(oneof = "volume_capability::AccessType", tags = "1, 2")]
    pub access_type: ::std::option::Option<volume_capability::AccessType>,
}
pub mod volume_capability {
    /// Indicate that the volume will be accessed via the block device API.
    ///
    /// Intentionally empty, for now.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BlockVolume {}
    /// Indicate that the volume will be accessed via the filesystem API.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct MountVolume {
        /// The filesystem type. This field is OPTIONAL.
        /// An empty string is equal to an unspecified field value.
        #[prost(string, tag = "1")]
        pub fs_type: std::string::String,
        /// The mount options that can be used for the volume. This field is
        /// OPTIONAL. `mount_flags` MAY contain sensitive information.
        /// Therefore, the CO and the Plugin MUST NOT leak this information
        /// to untrusted entities. The total size of this repeated field
        /// SHALL NOT exceed 4 KiB.
        #[prost(string, repeated, tag = "2")]
        pub mount_flags: ::std::vec::Vec<std::string::String>,
    }
    /// Specify how a volume can be accessed.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AccessMode {
        /// This field is REQUIRED.
        #[prost(enumeration = "access_mode::Mode", tag = "1")]
        pub mode: i32,
    }
    pub mod access_mode {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Mode {
            Unknown = 0,
            /// Can only be published once as read/write on a single node, at
            /// any given time.
            SingleNodeWriter = 1,
            /// Can only be published once as readonly on a single node, at
            /// any given time.
            SingleNodeReaderOnly = 2,
            /// Can be published as readonly at multiple nodes simultaneously.
            MultiNodeReaderOnly = 3,
            /// Can be published at multiple nodes simultaneously. Only one of
            /// the node can be used as read/write. The rest will be readonly.
            MultiNodeSingleWriter = 4,
            /// Can be published as read/write at multiple nodes
            /// simultaneously.
            MultiNodeMultiWriter = 5,
        }
    }
    /// Specifies what API the volume will be accessed using. One of the
    /// following fields MUST be specified.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum AccessType {
        #[prost(message, tag = "1")]
        Block(BlockVolume),
        #[prost(message, tag = "2")]
        Mount(MountVolume),
    }
}
/// The capacity of the storage space in bytes. To specify an exact size,
/// `required_bytes` and `limit_bytes` SHALL be set to the same value. At
/// least one of the these fields MUST be specified.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CapacityRange {
    /// Volume MUST be at least this big. This field is OPTIONAL.
    /// A value of 0 is equal to an unspecified field value.
    /// The value of this field MUST NOT be negative.
    #[prost(int64, tag = "1")]
    pub required_bytes: i64,
    /// Volume MUST not be bigger than this. This field is OPTIONAL.
    /// A value of 0 is equal to an unspecified field value.
    /// The value of this field MUST NOT be negative.
    #[prost(int64, tag = "2")]
    pub limit_bytes: i64,
}
/// Information about a specific volume.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Volume {
    /// The capacity of the volume in bytes. This field is OPTIONAL. If not
    /// set (value of 0), it indicates that the capacity of the volume is
    /// unknown (e.g., NFS share).
    /// The value of this field MUST NOT be negative.
    #[prost(int64, tag = "1")]
    pub capacity_bytes: i64,
    /// The identifier for this volume, generated by the plugin.
    /// This field is REQUIRED.
    /// This field MUST contain enough information to uniquely identify
    /// this specific volume vs all other volumes supported by this plugin.
    /// This field SHALL be used by the CO in subsequent calls to refer to
    /// this volume.
    /// The SP is NOT responsible for global uniqueness of volume_id across
    /// multiple SPs.
    #[prost(string, tag = "2")]
    pub volume_id: std::string::String,
    /// Opaque static properties of the volume. SP MAY use this field to
    /// ensure subsequent volume validation and publishing calls have
    /// contextual information.
    /// The contents of this field SHALL be opaque to a CO.
    /// The contents of this field SHALL NOT be mutable.
    /// The contents of this field SHALL be safe for the CO to cache.
    /// The contents of this field SHOULD NOT contain sensitive
    /// information.
    /// The contents of this field SHOULD NOT be used for uniquely
    /// identifying a volume. The `volume_id` alone SHOULD be sufficient to
    /// identify the volume.
    /// A volume uniquely identified by `volume_id` SHALL always report the
    /// same volume_context.
    /// This field is OPTIONAL and when present MUST be passed to volume
    /// validation and publishing calls.
    #[prost(map = "string, string", tag = "3")]
    pub volume_context: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// If specified, indicates that the volume is not empty and is
    /// pre-populated with data from the specified source.
    /// This field is OPTIONAL.
    #[prost(message, optional, tag = "4")]
    pub content_source: ::std::option::Option<VolumeContentSource>,
    /// Specifies where (regions, zones, racks, etc.) the provisioned
    /// volume is accessible from.
    /// A plugin that returns this field MUST also set the
    /// VOLUME_ACCESSIBILITY_CONSTRAINTS plugin capability.
    /// An SP MAY specify multiple topologies to indicate the volume is
    /// accessible from multiple locations.
    /// COs MAY use this information along with the topology information
    /// returned by NodeGetInfo to ensure that a given volume is accessible
    /// from a given node when scheduling workloads.
    /// This field is OPTIONAL. If it is not specified, the CO MAY assume
    /// the volume is equally accessible from all nodes in the cluster and
    /// MAY schedule workloads referencing the volume on any available
    /// node.
    ///
    /// Example 1:
    ///   accessible_topology = {"region": "R1", "zone": "Z2"}
    /// Indicates a volume accessible only from the "region" "R1" and the
    /// "zone" "Z2".
    ///
    /// Example 2:
    ///   accessible_topology =
    ///     {"region": "R1", "zone": "Z2"},
    ///     {"region": "R1", "zone": "Z3"}
    /// Indicates a volume accessible from both "zone" "Z2" and "zone" "Z3"
    /// in the "region" "R1".
    #[prost(message, repeated, tag = "5")]
    pub accessible_topology: ::std::vec::Vec<Topology>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopologyRequirement {
    /// Specifies the list of topologies the provisioned volume MUST be
    /// accessible from.
    /// This field is OPTIONAL. If TopologyRequirement is specified either
    /// requisite or preferred or both MUST be specified.
    ///
    /// If requisite is specified, the provisioned volume MUST be
    /// accessible from at least one of the requisite topologies.
    ///
    /// Given
    ///   x = number of topologies provisioned volume is accessible from
    ///   n = number of requisite topologies
    /// The CO MUST ensure n >= 1. The SP MUST ensure x >= 1
    /// If x==n, then the SP MUST make the provisioned volume available to
    /// all topologies from the list of requisite topologies. If it is
    /// unable to do so, the SP MUST fail the CreateVolume call.
    /// For example, if a volume should be accessible from a single zone,
    /// and requisite =
    ///   {"region": "R1", "zone": "Z2"}
    /// then the provisioned volume MUST be accessible from the "region"
    /// "R1" and the "zone" "Z2".
    /// Similarly, if a volume should be accessible from two zones, and
    /// requisite =
    ///   {"region": "R1", "zone": "Z2"},
    ///   {"region": "R1", "zone": "Z3"}
    /// then the provisioned volume MUST be accessible from the "region"
    /// "R1" and both "zone" "Z2" and "zone" "Z3".
    ///
    /// If x<n, then the SP SHALL choose x unique topologies from the list
    /// of requisite topologies. If it is unable to do so, the SP MUST fail
    /// the CreateVolume call.
    /// For example, if a volume should be accessible from a single zone,
    /// and requisite =
    ///   {"region": "R1", "zone": "Z2"},
    ///   {"region": "R1", "zone": "Z3"}
    /// then the SP may choose to make the provisioned volume available in
    /// either the "zone" "Z2" or the "zone" "Z3" in the "region" "R1".
    /// Similarly, if a volume should be accessible from two zones, and
    /// requisite =
    ///   {"region": "R1", "zone": "Z2"},
    ///   {"region": "R1", "zone": "Z3"},
    ///   {"region": "R1", "zone": "Z4"}
    /// then the provisioned volume MUST be accessible from any combination
    /// of two unique topologies: e.g. "R1/Z2" and "R1/Z3", or "R1/Z2" and
    ///  "R1/Z4", or "R1/Z3" and "R1/Z4".
    ///
    /// If x>n, then the SP MUST make the provisioned volume available from
    /// all topologies from the list of requisite topologies and MAY choose
    /// the remaining x-n unique topologies from the list of all possible
    /// topologies. If it is unable to do so, the SP MUST fail the
    /// CreateVolume call.
    /// For example, if a volume should be accessible from two zones, and
    /// requisite =
    ///   {"region": "R1", "zone": "Z2"}
    /// then the provisioned volume MUST be accessible from the "region"
    /// "R1" and the "zone" "Z2" and the SP may select the second zone
    /// independently, e.g. "R1/Z4".
    #[prost(message, repeated, tag = "1")]
    pub requisite: ::std::vec::Vec<Topology>,
    /// Specifies the list of topologies the CO would prefer the volume to
    /// be provisioned in.
    ///
    /// This field is OPTIONAL. If TopologyRequirement is specified either
    /// requisite or preferred or both MUST be specified.
    ///
    /// An SP MUST attempt to make the provisioned volume available using
    /// the preferred topologies in order from first to last.
    ///
    /// If requisite is specified, all topologies in preferred list MUST
    /// also be present in the list of requisite topologies.
    ///
    /// If the SP is unable to to make the provisioned volume available
    /// from any of the preferred topologies, the SP MAY choose a topology
    /// from the list of requisite topologies.
    /// If the list of requisite topologies is not specified, then the SP
    /// MAY choose from the list of all possible topologies.
    /// If the list of requisite topologies is specified and the SP is
    /// unable to to make the provisioned volume available from any of the
    /// requisite topologies it MUST fail the CreateVolume call.
    ///
    /// Example 1:
    /// Given a volume should be accessible from a single zone, and
    /// requisite =
    ///   {"region": "R1", "zone": "Z2"},
    ///   {"region": "R1", "zone": "Z3"}
    /// preferred =
    ///   {"region": "R1", "zone": "Z3"}
    /// then the the SP SHOULD first attempt to make the provisioned volume
    /// available from "zone" "Z3" in the "region" "R1" and fall back to
    /// "zone" "Z2" in the "region" "R1" if that is not possible.
    ///
    /// Example 2:
    /// Given a volume should be accessible from a single zone, and
    /// requisite =
    ///   {"region": "R1", "zone": "Z2"},
    ///   {"region": "R1", "zone": "Z3"},
    ///   {"region": "R1", "zone": "Z4"},
    ///   {"region": "R1", "zone": "Z5"}
    /// preferred =
    ///   {"region": "R1", "zone": "Z4"},
    ///   {"region": "R1", "zone": "Z2"}
    /// then the the SP SHOULD first attempt to make the provisioned volume
    /// accessible from "zone" "Z4" in the "region" "R1" and fall back to
    /// "zone" "Z2" in the "region" "R1" if that is not possible. If that
    /// is not possible, the SP may choose between either the "zone"
    /// "Z3" or "Z5" in the "region" "R1".
    ///
    /// Example 3:
    /// Given a volume should be accessible from TWO zones (because an
    /// opaque parameter in CreateVolumeRequest, for example, specifies
    /// the volume is accessible from two zones, aka synchronously
    /// replicated), and
    /// requisite =
    ///   {"region": "R1", "zone": "Z2"},
    ///   {"region": "R1", "zone": "Z3"},
    ///   {"region": "R1", "zone": "Z4"},
    ///   {"region": "R1", "zone": "Z5"}
    /// preferred =
    ///   {"region": "R1", "zone": "Z5"},
    ///   {"region": "R1", "zone": "Z3"}
    /// then the the SP SHOULD first attempt to make the provisioned volume
    /// accessible from the combination of the two "zones" "Z5" and "Z3" in
    /// the "region" "R1". If that's not possible, it should fall back to
    /// a combination of "Z5" and other possibilities from the list of
    /// requisite. If that's not possible, it should fall back  to a
    /// combination of "Z3" and other possibilities from the list of
    /// requisite. If that's not possible, it should fall back  to a
    /// combination of other possibilities from the list of requisite.
    #[prost(message, repeated, tag = "2")]
    pub preferred: ::std::vec::Vec<Topology>,
}
/// Topology is a map of topological domains to topological segments.
/// A topological domain is a sub-division of a cluster, like "region",
/// "zone", "rack", etc.
/// A topological segment is a specific instance of a topological domain,
/// like "zone3", "rack3", etc.
/// For example {"com.company/zone": "Z1", "com.company/rack": "R3"}
/// Valid keys have two segments: an OPTIONAL prefix and name, separated
/// by a slash (/), for example: "com.company.example/zone".
/// The key name segment is REQUIRED. The prefix is OPTIONAL.
/// The key name MUST be 63 characters or less, begin and end with an
/// alphanumeric character ([a-z0-9A-Z]), and contain only dashes (-),
/// underscores (_), dots (.), or alphanumerics in between, for example
/// "zone".
/// The key prefix MUST be 63 characters or less, begin and end with a
/// lower-case alphanumeric character ([a-z0-9]), contain only
/// dashes (-), dots (.), or lower-case alphanumerics in between, and
/// follow domain name notation format
/// (https://tools.ietf.org/html/rfc1035#section-2.3.1).
/// The key prefix SHOULD include the plugin's host company name and/or
/// the plugin name, to minimize the possibility of collisions with keys
/// from other plugins.
/// If a key prefix is specified, it MUST be identical across all
/// topology keys returned by the SP (across all RPCs).
/// Keys MUST be case-insensitive. Meaning the keys "Zone" and "zone"
/// MUST not both exist.
/// Each value (topological segment) MUST contain 1 or more strings.
/// Each string MUST be 63 characters or less and begin and end with an
/// alphanumeric character with '-', '_', '.', or alphanumerics in
/// between.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Topology {
    #[prost(map = "string, string", tag = "1")]
    pub segments: ::std::collections::HashMap<std::string::String, std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteVolumeRequest {
    /// The ID of the volume to be deprovisioned.
    /// This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// Secrets required by plugin to complete volume deletion request.
    /// This field is OPTIONAL. Refer to the `Secrets Requirements`
    /// section on how to use this field.
    #[prost(map = "string, string", tag = "2")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteVolumeResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerPublishVolumeRequest {
    /// The ID of the volume to be used on a node.
    /// This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// The ID of the node. This field is REQUIRED. The CO SHALL set this
    /// field to match the node ID returned by `NodeGetInfo`.
    #[prost(string, tag = "2")]
    pub node_id: std::string::String,
    /// Volume capability describing how the CO intends to use this volume.
    /// SP MUST ensure the CO can use the published volume as described.
    /// Otherwise SP MUST return the appropriate gRPC error code.
    /// This is a REQUIRED field.
    #[prost(message, optional, tag = "3")]
    pub volume_capability: ::std::option::Option<VolumeCapability>,
    /// Indicates SP MUST publish the volume in readonly mode.
    /// CO MUST set this field to false if SP does not have the
    /// PUBLISH_READONLY controller capability.
    /// This is a REQUIRED field.
    #[prost(bool, tag = "4")]
    pub readonly: bool,
    /// Secrets required by plugin to complete controller publish volume
    /// request. This field is OPTIONAL. Refer to the
    /// `Secrets Requirements` section on how to use this field.
    #[prost(map = "string, string", tag = "5")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// Volume context as returned by CO in CreateVolumeRequest. This field
    /// is OPTIONAL and MUST match the volume_context of the volume
    /// identified by `volume_id`.
    #[prost(map = "string, string", tag = "6")]
    pub volume_context: ::std::collections::HashMap<std::string::String, std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerPublishVolumeResponse {
    /// Opaque static publish properties of the volume. SP MAY use this
    /// field to ensure subsequent `NodeStageVolume` or `NodePublishVolume`
    /// calls calls have contextual information.
    /// The contents of this field SHALL be opaque to a CO.
    /// The contents of this field SHALL NOT be mutable.
    /// The contents of this field SHALL be safe for the CO to cache.
    /// The contents of this field SHOULD NOT contain sensitive
    /// information.
    /// The contents of this field SHOULD NOT be used for uniquely
    /// identifying a volume. The `volume_id` alone SHOULD be sufficient to
    /// identify the volume.
    /// This field is OPTIONAL and when present MUST be passed to
    /// subsequent `NodeStageVolume` or `NodePublishVolume` calls
    #[prost(map = "string, string", tag = "1")]
    pub publish_context: ::std::collections::HashMap<std::string::String, std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerUnpublishVolumeRequest {
    /// The ID of the volume. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// The ID of the node. This field is OPTIONAL. The CO SHOULD set this
    /// field to match the node ID returned by `NodeGetInfo` or leave it
    /// unset. If the value is set, the SP MUST unpublish the volume from
    /// the specified node. If the value is unset, the SP MUST unpublish
    /// the volume from all nodes it is published to.
    #[prost(string, tag = "2")]
    pub node_id: std::string::String,
    /// Secrets required by plugin to complete controller unpublish volume
    /// request. This SHOULD be the same secrets passed to the
    /// ControllerPublishVolume call for the specified volume.
    /// This field is OPTIONAL. Refer to the `Secrets Requirements`
    /// section on how to use this field.
    #[prost(map = "string, string", tag = "3")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerUnpublishVolumeResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateVolumeCapabilitiesRequest {
    /// The ID of the volume to check. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// Volume context as returned by CO in CreateVolumeRequest. This field
    /// is OPTIONAL and MUST match the volume_context of the volume
    /// identified by `volume_id`.
    #[prost(map = "string, string", tag = "2")]
    pub volume_context: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// The capabilities that the CO wants to check for the volume. This
    /// call SHALL return "confirmed" only if all the volume capabilities
    /// specified below are supported. This field is REQUIRED.
    #[prost(message, repeated, tag = "3")]
    pub volume_capabilities: ::std::vec::Vec<VolumeCapability>,
    /// See CreateVolumeRequest.parameters.
    /// This field is OPTIONAL.
    #[prost(map = "string, string", tag = "4")]
    pub parameters: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// Secrets required by plugin to complete volume validation request.
    /// This field is OPTIONAL. Refer to the `Secrets Requirements`
    /// section on how to use this field.
    #[prost(map = "string, string", tag = "5")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateVolumeCapabilitiesResponse {
    /// Confirmed indicates to the CO the set of capabilities that the
    /// plugin has validated. This field SHALL only be set to a non-empty
    /// value for successful validation responses.
    /// For successful validation responses, the CO SHALL compare the
    /// fields of this message to the originally requested capabilities in
    /// order to guard against an older plugin reporting "valid" for newer
    /// capability fields that it does not yet understand.
    /// This field is OPTIONAL.
    #[prost(message, optional, tag = "1")]
    pub confirmed: ::std::option::Option<validate_volume_capabilities_response::Confirmed>,
    /// Message to the CO if `confirmed` above is empty. This field is
    /// OPTIONAL.
    /// An empty string is equal to an unspecified field value.
    #[prost(string, tag = "2")]
    pub message: std::string::String,
}
pub mod validate_volume_capabilities_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Confirmed {
        /// Volume context validated by the plugin.
        /// This field is OPTIONAL.
        #[prost(map = "string, string", tag = "1")]
        pub volume_context: ::std::collections::HashMap<std::string::String, std::string::String>,
        /// Volume capabilities supported by the plugin.
        /// This field is REQUIRED.
        #[prost(message, repeated, tag = "2")]
        pub volume_capabilities: ::std::vec::Vec<super::VolumeCapability>,
        /// The volume creation parameters validated by the plugin.
        /// This field is OPTIONAL.
        #[prost(map = "string, string", tag = "3")]
        pub parameters: ::std::collections::HashMap<std::string::String, std::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListVolumesRequest {
    /// If specified (non-zero value), the Plugin MUST NOT return more
    /// entries than this number in the response. If the actual number of
    /// entries is more than this number, the Plugin MUST set `next_token`
    /// in the response which can be used to get the next page of entries
    /// in the subsequent `ListVolumes` call. This field is OPTIONAL. If
    /// not specified (zero value), it means there is no restriction on the
    /// number of entries that can be returned.
    /// The value of this field MUST NOT be negative.
    #[prost(int32, tag = "1")]
    pub max_entries: i32,
    /// A token to specify where to start paginating. Set this field to
    /// `next_token` returned by a previous `ListVolumes` call to get the
    /// next page of entries. This field is OPTIONAL.
    /// An empty string is equal to an unspecified field value.
    #[prost(string, tag = "2")]
    pub starting_token: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListVolumesResponse {
    #[prost(message, repeated, tag = "1")]
    pub entries: ::std::vec::Vec<list_volumes_response::Entry>,
    /// This token allows you to get the next page of entries for
    /// `ListVolumes` request. If the number of entries is larger than
    /// `max_entries`, use the `next_token` as a value for the
    /// `starting_token` field in the next `ListVolumes` request. This
    /// field is OPTIONAL.
    /// An empty string is equal to an unspecified field value.
    #[prost(string, tag = "2")]
    pub next_token: std::string::String,
}
pub mod list_volumes_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Entry {
        #[prost(message, optional, tag = "1")]
        pub volume: ::std::option::Option<super::Volume>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCapacityRequest {
    /// If specified, the Plugin SHALL report the capacity of the storage
    /// that can be used to provision volumes that satisfy ALL of the
    /// specified `volume_capabilities`. These are the same
    /// `volume_capabilities` the CO will use in `CreateVolumeRequest`.
    /// This field is OPTIONAL.
    #[prost(message, repeated, tag = "1")]
    pub volume_capabilities: ::std::vec::Vec<VolumeCapability>,
    /// If specified, the Plugin SHALL report the capacity of the storage
    /// that can be used to provision volumes with the given Plugin
    /// specific `parameters`. These are the same `parameters` the CO will
    /// use in `CreateVolumeRequest`. This field is OPTIONAL.
    #[prost(map = "string, string", tag = "2")]
    pub parameters: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// If specified, the Plugin SHALL report the capacity of the storage
    /// that can be used to provision volumes that in the specified
    /// `accessible_topology`. This is the same as the
    /// `accessible_topology` the CO returns in a `CreateVolumeResponse`.
    /// This field is OPTIONAL. This field SHALL NOT be set unless the
    /// plugin advertises the VOLUME_ACCESSIBILITY_CONSTRAINTS capability.
    #[prost(message, optional, tag = "3")]
    pub accessible_topology: ::std::option::Option<Topology>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCapacityResponse {
    /// The available capacity, in bytes, of the storage that can be used
    /// to provision volumes. If `volume_capabilities` or `parameters` is
    /// specified in the request, the Plugin SHALL take those into
    /// consideration when calculating the available capacity of the
    /// storage. This field is REQUIRED.
    /// The value of this field MUST NOT be negative.
    #[prost(int64, tag = "1")]
    pub available_capacity: i64,
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerGetCapabilitiesRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerGetCapabilitiesResponse {
    /// All the capabilities that the controller service supports. This
    /// field is OPTIONAL.
    #[prost(message, repeated, tag = "1")]
    pub capabilities: ::std::vec::Vec<ControllerServiceCapability>,
}
/// Specifies a capability of the controller service.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerServiceCapability {
    #[prost(oneof = "controller_service_capability::Type", tags = "1")]
    pub r#type: ::std::option::Option<controller_service_capability::Type>,
}
pub mod controller_service_capability {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Rpc {
        #[prost(enumeration = "rpc::Type", tag = "1")]
        pub r#type: i32,
    }
    pub mod rpc {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Type {
            Unknown = 0,
            CreateDeleteVolume = 1,
            PublishUnpublishVolume = 2,
            ListVolumes = 3,
            GetCapacity = 4,
            /// Currently the only way to consume a snapshot is to create
            /// a volume from it. Therefore plugins supporting
            /// CREATE_DELETE_SNAPSHOT MUST support creating volume from
            /// snapshot.
            CreateDeleteSnapshot = 5,
            ListSnapshots = 6,
            /// Plugins supporting volume cloning at the storage level MAY
            /// report this capability. The source volume MUST be managed by
            /// the same plugin. Not all volume sources and parameters
            /// combinations MAY work.
            CloneVolume = 7,
            /// Indicates the SP supports ControllerPublishVolume.readonly
            /// field.
            PublishReadonly = 8,
            /// See VolumeExpansion for details.
            ExpandVolume = 9,
        }
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        /// RPC that the controller supports.
        #[prost(message, tag = "1")]
        Rpc(Rpc),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSnapshotRequest {
    /// The ID of the source volume to be snapshotted.
    /// This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub source_volume_id: std::string::String,
    /// The suggested name for the snapshot. This field is REQUIRED for
    /// idempotency.
    /// Any Unicode string that conforms to the length limit is allowed
    /// except those containing the following banned characters:
    /// U+0000-U+0008, U+000B, U+000C, U+000E-U+001F, U+007F-U+009F.
    /// (These are control characters other than commonly used whitespace.)
    #[prost(string, tag = "2")]
    pub name: std::string::String,
    /// Secrets required by plugin to complete snapshot creation request.
    /// This field is OPTIONAL. Refer to the `Secrets Requirements`
    /// section on how to use this field.
    #[prost(map = "string, string", tag = "3")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// Plugin specific parameters passed in as opaque key-value pairs.
    /// This field is OPTIONAL. The Plugin is responsible for parsing and
    /// validating these parameters. COs will treat these as opaque.
    /// Use cases for opaque parameters:
    /// - Specify a policy to automatically clean up the snapshot.
    /// - Specify an expiration date for the snapshot.
    /// - Specify whether the snapshot is readonly or read/write.
    /// - Specify if the snapshot should be replicated to some place.
    /// - Specify primary or secondary for replication systems that
    ///   support snapshotting only on primary.
    #[prost(map = "string, string", tag = "4")]
    pub parameters: ::std::collections::HashMap<std::string::String, std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSnapshotResponse {
    /// Contains all attributes of the newly created snapshot that are
    /// relevant to the CO along with information required by the Plugin
    /// to uniquely identify the snapshot. This field is REQUIRED.
    #[prost(message, optional, tag = "1")]
    pub snapshot: ::std::option::Option<Snapshot>,
}
/// Information about a specific snapshot.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Snapshot {
    /// This is the complete size of the snapshot in bytes. The purpose of
    /// this field is to give CO guidance on how much space is needed to
    /// create a volume from this snapshot. The size of the volume MUST NOT
    /// be less than the size of the source snapshot. This field is
    /// OPTIONAL. If this field is not set, it indicates that this size is
    /// unknown. The value of this field MUST NOT be negative and a size of
    /// zero means it is unspecified.
    #[prost(int64, tag = "1")]
    pub size_bytes: i64,
    /// The identifier for this snapshot, generated by the plugin.
    /// This field is REQUIRED.
    /// This field MUST contain enough information to uniquely identify
    /// this specific snapshot vs all other snapshots supported by this
    /// plugin.
    /// This field SHALL be used by the CO in subsequent calls to refer to
    /// this snapshot.
    /// The SP is NOT responsible for global uniqueness of snapshot_id
    /// across multiple SPs.
    #[prost(string, tag = "2")]
    pub snapshot_id: std::string::String,
    /// Identity information for the source volume. Note that creating a
    /// snapshot from a snapshot is not supported here so the source has to
    /// be a volume. This field is REQUIRED.
    #[prost(string, tag = "3")]
    pub source_volume_id: std::string::String,
    /// Timestamp when the point-in-time snapshot is taken on the storage
    /// system. This field is REQUIRED.
    #[prost(message, optional, tag = "4")]
    pub creation_time: ::std::option::Option<::prost_types::Timestamp>,
    /// Indicates if a snapshot is ready to use as a
    /// `volume_content_source` in a `CreateVolumeRequest`. The default
    /// value is false. This field is REQUIRED.
    #[prost(bool, tag = "5")]
    pub ready_to_use: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSnapshotRequest {
    /// The ID of the snapshot to be deleted.
    /// This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub snapshot_id: std::string::String,
    /// Secrets required by plugin to complete snapshot deletion request.
    /// This field is OPTIONAL. Refer to the `Secrets Requirements`
    /// section on how to use this field.
    #[prost(map = "string, string", tag = "2")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSnapshotResponse {}
/// List all snapshots on the storage system regardless of how they were
/// created.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSnapshotsRequest {
    /// If specified (non-zero value), the Plugin MUST NOT return more
    /// entries than this number in the response. If the actual number of
    /// entries is more than this number, the Plugin MUST set `next_token`
    /// in the response which can be used to get the next page of entries
    /// in the subsequent `ListSnapshots` call. This field is OPTIONAL. If
    /// not specified (zero value), it means there is no restriction on the
    /// number of entries that can be returned.
    /// The value of this field MUST NOT be negative.
    #[prost(int32, tag = "1")]
    pub max_entries: i32,
    /// A token to specify where to start paginating. Set this field to
    /// `next_token` returned by a previous `ListSnapshots` call to get the
    /// next page of entries. This field is OPTIONAL.
    /// An empty string is equal to an unspecified field value.
    #[prost(string, tag = "2")]
    pub starting_token: std::string::String,
    /// Identity information for the source volume. This field is OPTIONAL.
    /// It can be used to list snapshots by volume.
    #[prost(string, tag = "3")]
    pub source_volume_id: std::string::String,
    /// Identity information for a specific snapshot. This field is
    /// OPTIONAL. It can be used to list only a specific snapshot.
    /// ListSnapshots will return with current snapshot information
    /// and will not block if the snapshot is being processed after
    /// it is cut.
    #[prost(string, tag = "4")]
    pub snapshot_id: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSnapshotsResponse {
    #[prost(message, repeated, tag = "1")]
    pub entries: ::std::vec::Vec<list_snapshots_response::Entry>,
    /// This token allows you to get the next page of entries for
    /// `ListSnapshots` request. If the number of entries is larger than
    /// `max_entries`, use the `next_token` as a value for the
    /// `starting_token` field in the next `ListSnapshots` request. This
    /// field is OPTIONAL.
    /// An empty string is equal to an unspecified field value.
    #[prost(string, tag = "2")]
    pub next_token: std::string::String,
}
pub mod list_snapshots_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Entry {
        #[prost(message, optional, tag = "1")]
        pub snapshot: ::std::option::Option<super::Snapshot>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerExpandVolumeRequest {
    /// The ID of the volume to expand. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// This allows CO to specify the capacity requirements of the volume
    /// after expansion. This field is REQUIRED.
    #[prost(message, optional, tag = "2")]
    pub capacity_range: ::std::option::Option<CapacityRange>,
    /// Secrets required by the plugin for expanding the volume.
    /// This field is OPTIONAL.
    #[prost(map = "string, string", tag = "3")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ControllerExpandVolumeResponse {
    /// Capacity of volume after expansion. This field is REQUIRED.
    #[prost(int64, tag = "1")]
    pub capacity_bytes: i64,
    /// Whether node expansion is required for the volume. When true
    /// the CO MUST make NodeExpandVolume RPC call on the node. This field
    /// is REQUIRED.
    #[prost(bool, tag = "2")]
    pub node_expansion_required: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeStageVolumeRequest {
    /// The ID of the volume to publish. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// The CO SHALL set this field to the value returned by
    /// `ControllerPublishVolume` if the corresponding Controller Plugin
    /// has `PUBLISH_UNPUBLISH_VOLUME` controller capability, and SHALL be
    /// left unset if the corresponding Controller Plugin does not have
    /// this capability. This is an OPTIONAL field.
    #[prost(map = "string, string", tag = "2")]
    pub publish_context: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// The path to which the volume MAY be staged. It MUST be an
    /// absolute path in the root filesystem of the process serving this
    /// request, and MUST be a directory. The CO SHALL ensure that there
    /// is only one `staging_target_path` per volume. The CO SHALL ensure
    /// that the path is directory and that the process serving the
    /// request has `read` and `write` permission to that directory. The
    /// CO SHALL be responsible for creating the directory if it does not
    /// exist.
    /// This is a REQUIRED field.
    #[prost(string, tag = "3")]
    pub staging_target_path: std::string::String,
    /// Volume capability describing how the CO intends to use this volume.
    /// SP MUST ensure the CO can use the staged volume as described.
    /// Otherwise SP MUST return the appropriate gRPC error code.
    /// This is a REQUIRED field.
    #[prost(message, optional, tag = "4")]
    pub volume_capability: ::std::option::Option<VolumeCapability>,
    /// Secrets required by plugin to complete node stage volume request.
    /// This field is OPTIONAL. Refer to the `Secrets Requirements`
    /// section on how to use this field.
    #[prost(map = "string, string", tag = "5")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// Volume context as returned by CO in CreateVolumeRequest. This field
    /// is OPTIONAL and MUST match the volume_context of the volume
    /// identified by `volume_id`.
    #[prost(map = "string, string", tag = "6")]
    pub volume_context: ::std::collections::HashMap<std::string::String, std::string::String>,
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeStageVolumeResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeUnstageVolumeRequest {
    /// The ID of the volume. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// The path at which the volume was staged. It MUST be an absolute
    /// path in the root filesystem of the process serving this request.
    /// This is a REQUIRED field.
    #[prost(string, tag = "2")]
    pub staging_target_path: std::string::String,
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeUnstageVolumeResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodePublishVolumeRequest {
    /// The ID of the volume to publish. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// The CO SHALL set this field to the value returned by
    /// `ControllerPublishVolume` if the corresponding Controller Plugin
    /// has `PUBLISH_UNPUBLISH_VOLUME` controller capability, and SHALL be
    /// left unset if the corresponding Controller Plugin does not have
    /// this capability. This is an OPTIONAL field.
    #[prost(map = "string, string", tag = "2")]
    pub publish_context: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// The path to which the volume was staged by `NodeStageVolume`.
    /// It MUST be an absolute path in the root filesystem of the process
    /// serving this request.
    /// It MUST be set if the Node Plugin implements the
    /// `STAGE_UNSTAGE_VOLUME` node capability.
    /// This is an OPTIONAL field.
    #[prost(string, tag = "3")]
    pub staging_target_path: std::string::String,
    /// The path to which the volume will be published. It MUST be an
    /// absolute path in the root filesystem of the process serving this
    /// request. The CO SHALL ensure uniqueness of target_path per volume.
    /// The CO SHALL ensure that the parent directory of this path exists
    /// and that the process serving the request has `read` and `write`
    /// permissions to that parent directory.
    /// For volumes with an access type of block, the SP SHALL place the
    /// block device at target_path.
    /// For volumes with an access type of mount, the SP SHALL place the
    /// mounted directory at target_path.
    /// Creation of target_path is the responsibility of the SP.
    /// This is a REQUIRED field.
    #[prost(string, tag = "4")]
    pub target_path: std::string::String,
    /// Volume capability describing how the CO intends to use this volume.
    /// SP MUST ensure the CO can use the published volume as described.
    /// Otherwise SP MUST return the appropriate gRPC error code.
    /// This is a REQUIRED field.
    #[prost(message, optional, tag = "5")]
    pub volume_capability: ::std::option::Option<VolumeCapability>,
    /// Indicates SP MUST publish the volume in readonly mode.
    /// This field is REQUIRED.
    #[prost(bool, tag = "6")]
    pub readonly: bool,
    /// Secrets required by plugin to complete node publish volume request.
    /// This field is OPTIONAL. Refer to the `Secrets Requirements`
    /// section on how to use this field.
    #[prost(map = "string, string", tag = "7")]
    pub secrets: ::std::collections::HashMap<std::string::String, std::string::String>,
    /// Volume context as returned by CO in CreateVolumeRequest. This field
    /// is OPTIONAL and MUST match the volume_context of the volume
    /// identified by `volume_id`.
    #[prost(map = "string, string", tag = "8")]
    pub volume_context: ::std::collections::HashMap<std::string::String, std::string::String>,
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodePublishVolumeResponse {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeUnpublishVolumeRequest {
    /// The ID of the volume. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// The path at which the volume was published. It MUST be an absolute
    /// path in the root filesystem of the process serving this request.
    /// The SP MUST delete the file or directory it created at this path.
    /// This is a REQUIRED field.
    #[prost(string, tag = "2")]
    pub target_path: std::string::String,
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeUnpublishVolumeResponse {}
// additional methods for listing
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeListVolumesRequest {}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeListVolumesResponse {
    #[prost(string, repeated, tag = "1")]
    pub vol_names: ::std::vec::Vec<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeGetVolumeStatsRequest {
    /// The ID of the volume. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// It can be any valid path where volume was previously
    /// staged or published.
    /// It MUST be an absolute path in the root filesystem of
    /// the process serving this request.
    /// This is a REQUIRED field.
    #[prost(string, tag = "2")]
    pub volume_path: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeGetVolumeStatsResponse {
    /// This field is OPTIONAL.
    #[prost(message, repeated, tag = "1")]
    pub usage: ::std::vec::Vec<VolumeUsage>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VolumeUsage {
    /// The available capacity in specified Unit. This field is OPTIONAL.
    /// The value of this field MUST NOT be negative.
    #[prost(int64, tag = "1")]
    pub available: i64,
    /// The total capacity in specified Unit. This field is REQUIRED.
    /// The value of this field MUST NOT be negative.
    #[prost(int64, tag = "2")]
    pub total: i64,
    /// The used capacity in specified Unit. This field is OPTIONAL.
    /// The value of this field MUST NOT be negative.
    #[prost(int64, tag = "3")]
    pub used: i64,
    /// Units by which values are measured. This field is REQUIRED.
    #[prost(enumeration = "volume_usage::Unit", tag = "4")]
    pub unit: i32,
}
pub mod volume_usage {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Unit {
        Unknown = 0,
        Bytes = 1,
        Inodes = 2,
    }
}
/// Intentionally empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeGetCapabilitiesRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeGetCapabilitiesResponse {
    /// All the capabilities that the node service supports. This field
    /// is OPTIONAL.
    #[prost(message, repeated, tag = "1")]
    pub capabilities: ::std::vec::Vec<NodeServiceCapability>,
}
/// Specifies a capability of the node service.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeServiceCapability {
    #[prost(oneof = "node_service_capability::Type", tags = "1")]
    pub r#type: ::std::option::Option<node_service_capability::Type>,
}
pub mod node_service_capability {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Rpc {
        #[prost(enumeration = "rpc::Type", tag = "1")]
        pub r#type: i32,
    }
    pub mod rpc {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Type {
            Unknown = 0,
            StageUnstageVolume = 1,
            /// If Plugin implements GET_VOLUME_STATS capability
            /// then it MUST implement NodeGetVolumeStats RPC
            /// call for fetching volume statistics.
            GetVolumeStats = 2,
            /// See VolumeExpansion for details.
            ExpandVolume = 3,
        }
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        /// RPC that the controller supports.
        #[prost(message, tag = "1")]
        Rpc(Rpc),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeGetInfoRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeGetInfoResponse {
    /// The identifier of the node as understood by the SP.
    /// This field is REQUIRED.
    /// This field MUST contain enough information to uniquely identify
    /// this specific node vs all other nodes supported by this plugin.
    /// This field SHALL be used by the CO in subsequent calls, including
    /// `ControllerPublishVolume`, to refer to this node.
    /// The SP is NOT responsible for global uniqueness of node_id across
    /// multiple SPs.
    #[prost(string, tag = "1")]
    pub node_id: std::string::String,
    /// Maximum number of volumes that controller can publish to the node.
    /// If value is not set or zero CO SHALL decide how many volumes of
    /// this type can be published by the controller to the node. The
    /// plugin MUST NOT set negative values here.
    /// This field is OPTIONAL.
    #[prost(int64, tag = "2")]
    pub max_volumes_per_node: i64,
    /// Specifies where (regions, zones, racks, etc.) the node is
    /// accessible from.
    /// A plugin that returns this field MUST also set the
    /// VOLUME_ACCESSIBILITY_CONSTRAINTS plugin capability.
    /// COs MAY use this information along with the topology information
    /// returned in CreateVolumeResponse to ensure that a given volume is
    /// accessible from a given node when scheduling workloads.
    /// This field is OPTIONAL. If it is not specified, the CO MAY assume
    /// the node is not subject to any topological constraint, and MAY
    /// schedule workloads that reference any volume V, such that there are
    /// no topological constraints declared for V.
    ///
    /// Example 1:
    ///   accessible_topology =
    ///     {"region": "R1", "zone": "R2"}
    /// Indicates the node exists within the "region" "R1" and the "zone"
    /// "Z2".
    #[prost(message, optional, tag = "3")]
    pub accessible_topology: ::std::option::Option<Topology>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeExpandVolumeRequest {
    /// The ID of the volume. This field is REQUIRED.
    #[prost(string, tag = "1")]
    pub volume_id: std::string::String,
    /// The path on which volume is available. This field is REQUIRED.
    #[prost(string, tag = "2")]
    pub volume_path: std::string::String,
    /// This allows CO to specify the capacity requirements of the volume
    /// after expansion. If capacity_range is omitted then a plugin MAY
    /// inspect the file system of the volume to determine the maximum
    /// capacity to which the volume can be expanded. In such cases a
    /// plugin MAY expand the volume to its maximum capacity.
    /// This field is OPTIONAL.
    #[prost(message, optional, tag = "3")]
    pub capacity_range: ::std::option::Option<CapacityRange>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeExpandVolumeResponse {
    /// The capacity of the volume in bytes. This field is OPTIONAL.
    #[prost(int64, tag = "1")]
    pub capacity_bytes: i64,
}
pub mod client {
    use super::{
        ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
        ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
        ControllerPublishVolumeRequest, ControllerPublishVolumeResponse,
        ControllerUnpublishVolumeRequest, ControllerUnpublishVolumeResponse, CreateSnapshotRequest,
        CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse, DeleteSnapshotRequest,
        DeleteSnapshotResponse, DeleteVolumeRequest, DeleteVolumeResponse, GetCapacityRequest,
        GetCapacityResponse, GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse,
        GetPluginInfoRequest, GetPluginInfoResponse, ListSnapshotsRequest, ListSnapshotsResponse,
        ListVolumesRequest, ListVolumesResponse, NodeExpandVolumeRequest, NodeExpandVolumeResponse,
        NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse, NodeGetInfoRequest,
        NodeGetInfoResponse, NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse,
        NodePublishVolumeRequest, NodePublishVolumeResponse, NodeStageVolumeRequest,
        NodeStageVolumeResponse, NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse,
        NodeListVolumesRequest, NodeListVolumesResponse,
        NodeUnstageVolumeRequest, NodeUnstageVolumeResponse, ProbeRequest, ProbeResponse,
        ValidateVolumeCapabilitiesRequest, ValidateVolumeCapabilitiesResponse,
    };
    use tower_grpc::codegen::client::*;

    #[derive(Debug, Clone)]
    pub struct Identity<T> {
        inner: grpc::Grpc<T>,
    }

    impl<T> Identity<T> {
        pub fn new(inner: T) -> Self {
            let inner = grpc::Grpc::new(inner);
            Self { inner }
        }

        /// Poll whether this client is ready to send another request.
        pub fn poll_ready<R>(&mut self) -> futures::Poll<(), grpc::Status>
        where
            T: grpc::GrpcService<R>,
        {
            self.inner.poll_ready()
        }

        /// Get a `Future` of when this client is ready to send another request.
        pub fn ready<R>(self) -> impl futures::Future<Item = Self, Error = grpc::Status>
        where
            T: grpc::GrpcService<R>,
        {
            futures::Future::map(self.inner.ready(), |inner| Self { inner })
        }

        pub fn get_plugin_info<R>(
            &mut self,
            request: grpc::Request<GetPluginInfoRequest>,
        ) -> grpc::unary::ResponseFuture<GetPluginInfoResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<GetPluginInfoRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Identity/GetPluginInfo");
            self.inner.unary(request, path)
        }

        pub fn get_plugin_capabilities<R>(
            &mut self,
            request: grpc::Request<GetPluginCapabilitiesRequest>,
        ) -> grpc::unary::ResponseFuture<GetPluginCapabilitiesResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<GetPluginCapabilitiesRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Identity/GetPluginCapabilities");
            self.inner.unary(request, path)
        }

        pub fn probe<R>(
            &mut self,
            request: grpc::Request<ProbeRequest>,
        ) -> grpc::unary::ResponseFuture<ProbeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<ProbeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Identity/Probe");
            self.inner.unary(request, path)
        }
    }

    #[derive(Debug, Clone)]
    pub struct Controller<T> {
        inner: grpc::Grpc<T>,
    }

    impl<T> Controller<T> {
        pub fn new(inner: T) -> Self {
            let inner = grpc::Grpc::new(inner);
            Self { inner }
        }

        /// Poll whether this client is ready to send another request.
        pub fn poll_ready<R>(&mut self) -> futures::Poll<(), grpc::Status>
        where
            T: grpc::GrpcService<R>,
        {
            self.inner.poll_ready()
        }

        /// Get a `Future` of when this client is ready to send another request.
        pub fn ready<R>(self) -> impl futures::Future<Item = Self, Error = grpc::Status>
        where
            T: grpc::GrpcService<R>,
        {
            futures::Future::map(self.inner.ready(), |inner| Self { inner })
        }

        pub fn create_volume<R>(
            &mut self,
            request: grpc::Request<CreateVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<CreateVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<CreateVolumeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Controller/CreateVolume");
            self.inner.unary(request, path)
        }

        pub fn delete_volume<R>(
            &mut self,
            request: grpc::Request<DeleteVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<DeleteVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<DeleteVolumeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Controller/DeleteVolume");
            self.inner.unary(request, path)
        }

        pub fn controller_publish_volume<R>(
            &mut self,
            request: grpc::Request<ControllerPublishVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<ControllerPublishVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<ControllerPublishVolumeRequest>: grpc::Encodable<R>,
        {
            let path =
                http::PathAndQuery::from_static("/csi.v1.Controller/ControllerPublishVolume");
            self.inner.unary(request, path)
        }

        pub fn controller_unpublish_volume<R>(
            &mut self,
            request: grpc::Request<ControllerUnpublishVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<
            ControllerUnpublishVolumeResponse,
            T::Future,
            T::ResponseBody,
        >
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<ControllerUnpublishVolumeRequest>: grpc::Encodable<R>,
        {
            let path =
                http::PathAndQuery::from_static("/csi.v1.Controller/ControllerUnpublishVolume");
            self.inner.unary(request, path)
        }

        pub fn validate_volume_capabilities<R>(
            &mut self,
            request: grpc::Request<ValidateVolumeCapabilitiesRequest>,
        ) -> grpc::unary::ResponseFuture<
            ValidateVolumeCapabilitiesResponse,
            T::Future,
            T::ResponseBody,
        >
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<ValidateVolumeCapabilitiesRequest>: grpc::Encodable<R>,
        {
            let path =
                http::PathAndQuery::from_static("/csi.v1.Controller/ValidateVolumeCapabilities");
            self.inner.unary(request, path)
        }

        pub fn list_volumes<R>(
            &mut self,
            request: grpc::Request<ListVolumesRequest>,
        ) -> grpc::unary::ResponseFuture<ListVolumesResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<ListVolumesRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Controller/ListVolumes");
            self.inner.unary(request, path)
        }

        pub fn get_capacity<R>(
            &mut self,
            request: grpc::Request<GetCapacityRequest>,
        ) -> grpc::unary::ResponseFuture<GetCapacityResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<GetCapacityRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Controller/GetCapacity");
            self.inner.unary(request, path)
        }

        pub fn controller_get_capabilities<R>(
            &mut self,
            request: grpc::Request<ControllerGetCapabilitiesRequest>,
        ) -> grpc::unary::ResponseFuture<
            ControllerGetCapabilitiesResponse,
            T::Future,
            T::ResponseBody,
        >
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<ControllerGetCapabilitiesRequest>: grpc::Encodable<R>,
        {
            let path =
                http::PathAndQuery::from_static("/csi.v1.Controller/ControllerGetCapabilities");
            self.inner.unary(request, path)
        }

        pub fn create_snapshot<R>(
            &mut self,
            request: grpc::Request<CreateSnapshotRequest>,
        ) -> grpc::unary::ResponseFuture<CreateSnapshotResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<CreateSnapshotRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Controller/CreateSnapshot");
            self.inner.unary(request, path)
        }

        pub fn delete_snapshot<R>(
            &mut self,
            request: grpc::Request<DeleteSnapshotRequest>,
        ) -> grpc::unary::ResponseFuture<DeleteSnapshotResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<DeleteSnapshotRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Controller/DeleteSnapshot");
            self.inner.unary(request, path)
        }

        pub fn list_snapshots<R>(
            &mut self,
            request: grpc::Request<ListSnapshotsRequest>,
        ) -> grpc::unary::ResponseFuture<ListSnapshotsResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<ListSnapshotsRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Controller/ListSnapshots");
            self.inner.unary(request, path)
        }

        pub fn controller_expand_volume<R>(
            &mut self,
            request: grpc::Request<ControllerExpandVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<ControllerExpandVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<ControllerExpandVolumeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Controller/ControllerExpandVolume");
            self.inner.unary(request, path)
        }
    }

    #[derive(Debug, Clone)]
    pub struct Node<T> {
        inner: grpc::Grpc<T>,
    }

    impl<T> Node<T> {
        pub fn new(inner: T) -> Self {
            let inner = grpc::Grpc::new(inner);
            Self { inner }
        }

        /// Poll whether this client is ready to send another request.
        pub fn poll_ready<R>(&mut self) -> futures::Poll<(), grpc::Status>
        where
            T: grpc::GrpcService<R>,
        {
            self.inner.poll_ready()
        }

        /// Get a `Future` of when this client is ready to send another request.
        pub fn ready<R>(self) -> impl futures::Future<Item = Self, Error = grpc::Status>
        where
            T: grpc::GrpcService<R>,
        {
            futures::Future::map(self.inner.ready(), |inner| Self { inner })
        }

        pub fn node_stage_volume<R>(
            &mut self,
            request: grpc::Request<NodeStageVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<NodeStageVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodeStageVolumeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodeStageVolume");
            self.inner.unary(request, path)
        }

        pub fn node_unstage_volume<R>(
            &mut self,
            request: grpc::Request<NodeUnstageVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<NodeUnstageVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodeUnstageVolumeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodeUnstageVolume");
            self.inner.unary(request, path)
        }

        // new node list volumes
        pub fn node_list_volumes<R>(
            &mut self,
            request: grpc::Request<NodeListVolumesRequest>,
        ) -> grpc::unary::ResponseFuture<NodeListVolumesResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodeListVolumesRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodeListVolumes");
            self.inner.unary(request, path)
        }

        pub fn node_publish_volume<R>(
            &mut self,
            request: grpc::Request<NodePublishVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<NodePublishVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodePublishVolumeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodePublishVolume");
            self.inner.unary(request, path)
        }

        pub fn node_unpublish_volume<R>(
            &mut self,
            request: grpc::Request<NodeUnpublishVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<NodeUnpublishVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodeUnpublishVolumeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodeUnpublishVolume");
            self.inner.unary(request, path)
        }

        pub fn node_get_volume_stats<R>(
            &mut self,
            request: grpc::Request<NodeGetVolumeStatsRequest>,
        ) -> grpc::unary::ResponseFuture<NodeGetVolumeStatsResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodeGetVolumeStatsRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodeGetVolumeStats");
            self.inner.unary(request, path)
        }

        pub fn node_expand_volume<R>(
            &mut self,
            request: grpc::Request<NodeExpandVolumeRequest>,
        ) -> grpc::unary::ResponseFuture<NodeExpandVolumeResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodeExpandVolumeRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodeExpandVolume");
            self.inner.unary(request, path)
        }

        pub fn node_get_capabilities<R>(
            &mut self,
            request: grpc::Request<NodeGetCapabilitiesRequest>,
        ) -> grpc::unary::ResponseFuture<NodeGetCapabilitiesResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodeGetCapabilitiesRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodeGetCapabilities");
            self.inner.unary(request, path)
        }

        pub fn node_get_info<R>(
            &mut self,
            request: grpc::Request<NodeGetInfoRequest>,
        ) -> grpc::unary::ResponseFuture<NodeGetInfoResponse, T::Future, T::ResponseBody>
        where
            T: grpc::GrpcService<R>,
            grpc::unary::Once<NodeGetInfoRequest>: grpc::Encodable<R>,
        {
            let path = http::PathAndQuery::from_static("/csi.v1.Node/NodeGetInfo");
            self.inner.unary(request, path)
        }
    }
}

pub mod server {
    use super::{
        ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
        ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
        ControllerPublishVolumeRequest, ControllerPublishVolumeResponse,
        ControllerUnpublishVolumeRequest, ControllerUnpublishVolumeResponse, CreateSnapshotRequest,
        CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse, DeleteSnapshotRequest,
        DeleteSnapshotResponse, DeleteVolumeRequest, DeleteVolumeResponse, GetCapacityRequest,
        GetCapacityResponse, GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse,
        GetPluginInfoRequest, GetPluginInfoResponse, ListSnapshotsRequest, ListSnapshotsResponse,
        ListVolumesRequest, ListVolumesResponse, NodeExpandVolumeRequest, NodeExpandVolumeResponse,
        NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse, NodeGetInfoRequest,
        NodeGetInfoResponse, NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse,
        NodePublishVolumeRequest, NodePublishVolumeResponse, NodeStageVolumeRequest,
        NodeStageVolumeResponse, NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse,
        NodeListVolumesRequest, NodeListVolumesResponse,
        NodeUnstageVolumeRequest, NodeUnstageVolumeResponse, ProbeRequest, ProbeResponse,
        ValidateVolumeCapabilitiesRequest, ValidateVolumeCapabilitiesResponse,
    };
    use tower_grpc::codegen::server::*;

    // Redefine the try_ready macro so that it doesn't need to be explicitly
    // imported by the user of this generated code.
    macro_rules! try_ready {
        ($e:expr) => {
            match $e {
                Ok(futures::Async::Ready(t)) => t,
                Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                Err(e) => return Err(From::from(e)),
            }
        };
    }

    pub trait Identity: Clone {
        type GetPluginInfoFuture: futures::Future<
            Item = grpc::Response<GetPluginInfoResponse>,
            Error = grpc::Status,
        >;
        type GetPluginCapabilitiesFuture: futures::Future<
            Item = grpc::Response<GetPluginCapabilitiesResponse>,
            Error = grpc::Status,
        >;
        type ProbeFuture: futures::Future<
            Item = grpc::Response<ProbeResponse>,
            Error = grpc::Status,
        >;

        fn get_plugin_info(
            &mut self,
            request: grpc::Request<GetPluginInfoRequest>,
        ) -> Self::GetPluginInfoFuture;

        fn get_plugin_capabilities(
            &mut self,
            request: grpc::Request<GetPluginCapabilitiesRequest>,
        ) -> Self::GetPluginCapabilitiesFuture;

        fn probe(&mut self, request: grpc::Request<ProbeRequest>) -> Self::ProbeFuture;
    }

    #[derive(Debug, Clone)]
    pub struct IdentityServer<T> {
        identity: T,
    }

    impl<T> IdentityServer<T>
    where
        T: Identity,
    {
        pub fn new(identity: T) -> Self {
            Self { identity }
        }
    }

    impl<T> tower::Service<http::Request<grpc::BoxBody>> for IdentityServer<T>
    where
        T: Identity,
    {
        type Response = http::Response<identity::ResponseBody<T>>;
        type Error = grpc::Never;
        type Future = identity::ResponseFuture<T>;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            Ok(().into())
        }

        fn call(&mut self, request: http::Request<grpc::BoxBody>) -> Self::Future {
            use self::identity::Kind::*;

            match request.uri().path() {
                "/csi.v1.Identity/GetPluginInfo" => {
                    let service = identity::methods::GetPluginInfo(self.identity.clone());
                    let response = grpc::unary(service, request);
                    identity::ResponseFuture {
                        kind: GetPluginInfo(response),
                    }
                }
                "/csi.v1.Identity/GetPluginCapabilities" => {
                    let service = identity::methods::GetPluginCapabilities(self.identity.clone());
                    let response = grpc::unary(service, request);
                    identity::ResponseFuture {
                        kind: GetPluginCapabilities(response),
                    }
                }
                "/csi.v1.Identity/Probe" => {
                    let service = identity::methods::Probe(self.identity.clone());
                    let response = grpc::unary(service, request);
                    identity::ResponseFuture {
                        kind: Probe(response),
                    }
                }
                _ => identity::ResponseFuture {
                    kind: __Generated__Unimplemented(grpc::unimplemented(format!(
                        "unknown service: {:?}",
                        request.uri().path()
                    ))),
                },
            }
        }
    }

    impl<T> tower::Service<()> for IdentityServer<T>
    where
        T: Identity,
    {
        type Response = Self;
        type Error = grpc::Never;
        type Future = futures::FutureResult<Self::Response, Self::Error>;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            Ok(futures::Async::Ready(()))
        }

        fn call(&mut self, _target: ()) -> Self::Future {
            futures::ok(self.clone())
        }
    }

    impl<T> tower::Service<http::Request<tower_hyper::Body>> for IdentityServer<T>
    where
        T: Identity,
    {
        type Response = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Response;
        type Error = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Error;
        type Future = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Future;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            tower::Service::<http::Request<grpc::BoxBody>>::poll_ready(self)
        }

        fn call(&mut self, request: http::Request<tower_hyper::Body>) -> Self::Future {
            let request = request.map(|b| grpc::BoxBody::map_from(b));
            tower::Service::<http::Request<grpc::BoxBody>>::call(self, request)
        }
    }

    pub mod identity {
        use super::super::{GetPluginCapabilitiesRequest, GetPluginInfoRequest, ProbeRequest};
        use super::Identity;
        use tower_grpc::codegen::server::*;

        pub struct ResponseFuture<T>
        where
            T: Identity,
        {
            pub(super) kind: Kind<
                // GetPluginInfo
                grpc::unary::ResponseFuture<
                    methods::GetPluginInfo<T>,
                    grpc::BoxBody,
                    GetPluginInfoRequest,
                >,
                // GetPluginCapabilities
                grpc::unary::ResponseFuture<
                    methods::GetPluginCapabilities<T>,
                    grpc::BoxBody,
                    GetPluginCapabilitiesRequest,
                >,
                // Probe
                grpc::unary::ResponseFuture<methods::Probe<T>, grpc::BoxBody, ProbeRequest>,
                // A generated catch-all for unimplemented service calls
                grpc::unimplemented::ResponseFuture,
            >,
        }

        impl<T> futures::Future for ResponseFuture<T>
        where
            T: Identity,
        {
            type Item = http::Response<ResponseBody<T>>;
            type Error = grpc::Never;

            fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    GetPluginInfo(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: GetPluginInfo(body),
                        });
                        Ok(response.into())
                    }
                    GetPluginCapabilities(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: GetPluginCapabilities(body),
                        });
                        Ok(response.into())
                    }
                    Probe(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody { kind: Probe(body) });
                        Ok(response.into())
                    }
                    __Generated__Unimplemented(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: __Generated__Unimplemented(body),
                        });
                        Ok(response.into())
                    }
                }
            }
        }

        pub struct ResponseBody<T>
        where
            T: Identity,
        {
            pub(super) kind:
                Kind<
                    // GetPluginInfo
                    grpc::Encode<
                        grpc::unary::Once<
                            <methods::GetPluginInfo<T> as grpc::UnaryService<
                                GetPluginInfoRequest,
                            >>::Response,
                        >,
                    >,
                    // GetPluginCapabilities
                    grpc::Encode<
                        grpc::unary::Once<
                            <methods::GetPluginCapabilities<T> as grpc::UnaryService<
                                GetPluginCapabilitiesRequest,
                            >>::Response,
                        >,
                    >,
                    // Probe
                    grpc::Encode<
                        grpc::unary::Once<
                            <methods::Probe<T> as grpc::UnaryService<ProbeRequest>>::Response,
                        >,
                    >,
                    // A generated catch-all for unimplemented service calls
                    (),
                >,
        }

        impl<T> tower::HttpBody for ResponseBody<T>
        where
            T: Identity,
        {
            type Data = <grpc::BoxBody as grpc::Body>::Data;
            type Error = grpc::Status;

            fn is_end_stream(&self) -> bool {
                use self::Kind::*;

                match self.kind {
                    GetPluginInfo(ref v) => v.is_end_stream(),
                    GetPluginCapabilities(ref v) => v.is_end_stream(),
                    Probe(ref v) => v.is_end_stream(),
                    __Generated__Unimplemented(_) => true,
                }
            }

            fn poll_data(&mut self) -> futures::Poll<Option<Self::Data>, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    GetPluginInfo(ref mut v) => v.poll_data(),
                    GetPluginCapabilities(ref mut v) => v.poll_data(),
                    Probe(ref mut v) => v.poll_data(),
                    __Generated__Unimplemented(_) => Ok(None.into()),
                }
            }

            fn poll_trailers(&mut self) -> futures::Poll<Option<http::HeaderMap>, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    GetPluginInfo(ref mut v) => v.poll_trailers(),
                    GetPluginCapabilities(ref mut v) => v.poll_trailers(),
                    Probe(ref mut v) => v.poll_trailers(),
                    __Generated__Unimplemented(_) => Ok(None.into()),
                }
            }
        }

        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone)]
        pub(super) enum Kind<
            GetPluginInfo,
            GetPluginCapabilities,
            Probe,
            __Generated__Unimplemented,
        > {
            GetPluginInfo(GetPluginInfo),
            GetPluginCapabilities(GetPluginCapabilities),
            Probe(Probe),
            __Generated__Unimplemented(__Generated__Unimplemented),
        }

        pub mod methods {
            use super::super::{
                GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
                GetPluginInfoResponse, Identity, ProbeRequest, ProbeResponse,
            };
            use tower_grpc::codegen::server::*;

            pub struct GetPluginInfo<T>(pub T);

            impl<T> tower::Service<grpc::Request<GetPluginInfoRequest>> for GetPluginInfo<T>
            where
                T: Identity,
            {
                type Response = grpc::Response<GetPluginInfoResponse>;
                type Error = grpc::Status;
                type Future = T::GetPluginInfoFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<GetPluginInfoRequest>) -> Self::Future {
                    self.0.get_plugin_info(request)
                }
            }

            pub struct GetPluginCapabilities<T>(pub T);

            impl<T> tower::Service<grpc::Request<GetPluginCapabilitiesRequest>> for GetPluginCapabilities<T>
            where
                T: Identity,
            {
                type Response = grpc::Response<GetPluginCapabilitiesResponse>;
                type Error = grpc::Status;
                type Future = T::GetPluginCapabilitiesFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<GetPluginCapabilitiesRequest>,
                ) -> Self::Future {
                    self.0.get_plugin_capabilities(request)
                }
            }

            pub struct Probe<T>(pub T);

            impl<T> tower::Service<grpc::Request<ProbeRequest>> for Probe<T>
            where
                T: Identity,
            {
                type Response = grpc::Response<ProbeResponse>;
                type Error = grpc::Status;
                type Future = T::ProbeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<ProbeRequest>) -> Self::Future {
                    self.0.probe(request)
                }
            }
        }
    }

    // Redefine the try_ready macro so that it doesn't need to be explicitly
    // imported by the user of this generated code.
    macro_rules! try_ready {
        ($e:expr) => {
            match $e {
                Ok(futures::Async::Ready(t)) => t,
                Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                Err(e) => return Err(From::from(e)),
            }
        };
    }

    pub trait Controller: Clone {
        type CreateVolumeFuture: futures::Future<
            Item = grpc::Response<CreateVolumeResponse>,
            Error = grpc::Status,
        >;
        type DeleteVolumeFuture: futures::Future<
            Item = grpc::Response<DeleteVolumeResponse>,
            Error = grpc::Status,
        >;
        type ControllerPublishVolumeFuture: futures::Future<
            Item = grpc::Response<ControllerPublishVolumeResponse>,
            Error = grpc::Status,
        >;
        type ControllerUnpublishVolumeFuture: futures::Future<
            Item = grpc::Response<ControllerUnpublishVolumeResponse>,
            Error = grpc::Status,
        >;
        type ValidateVolumeCapabilitiesFuture: futures::Future<
            Item = grpc::Response<ValidateVolumeCapabilitiesResponse>,
            Error = grpc::Status,
        >;
        type ListVolumesFuture: futures::Future<
            Item = grpc::Response<ListVolumesResponse>,
            Error = grpc::Status,
        >;
        type GetCapacityFuture: futures::Future<
            Item = grpc::Response<GetCapacityResponse>,
            Error = grpc::Status,
        >;
        type ControllerGetCapabilitiesFuture: futures::Future<
            Item = grpc::Response<ControllerGetCapabilitiesResponse>,
            Error = grpc::Status,
        >;
        type CreateSnapshotFuture: futures::Future<
            Item = grpc::Response<CreateSnapshotResponse>,
            Error = grpc::Status,
        >;
        type DeleteSnapshotFuture: futures::Future<
            Item = grpc::Response<DeleteSnapshotResponse>,
            Error = grpc::Status,
        >;
        type ListSnapshotsFuture: futures::Future<
            Item = grpc::Response<ListSnapshotsResponse>,
            Error = grpc::Status,
        >;
        type ControllerExpandVolumeFuture: futures::Future<
            Item = grpc::Response<ControllerExpandVolumeResponse>,
            Error = grpc::Status,
        >;

        fn create_volume(
            &mut self,
            request: grpc::Request<CreateVolumeRequest>,
        ) -> Self::CreateVolumeFuture;

        fn delete_volume(
            &mut self,
            request: grpc::Request<DeleteVolumeRequest>,
        ) -> Self::DeleteVolumeFuture;

        fn controller_publish_volume(
            &mut self,
            request: grpc::Request<ControllerPublishVolumeRequest>,
        ) -> Self::ControllerPublishVolumeFuture;

        fn controller_unpublish_volume(
            &mut self,
            request: grpc::Request<ControllerUnpublishVolumeRequest>,
        ) -> Self::ControllerUnpublishVolumeFuture;

        fn validate_volume_capabilities(
            &mut self,
            request: grpc::Request<ValidateVolumeCapabilitiesRequest>,
        ) -> Self::ValidateVolumeCapabilitiesFuture;

        fn list_volumes(
            &mut self,
            request: grpc::Request<ListVolumesRequest>,
        ) -> Self::ListVolumesFuture;

        fn get_capacity(
            &mut self,
            request: grpc::Request<GetCapacityRequest>,
        ) -> Self::GetCapacityFuture;

        fn controller_get_capabilities(
            &mut self,
            request: grpc::Request<ControllerGetCapabilitiesRequest>,
        ) -> Self::ControllerGetCapabilitiesFuture;

        fn create_snapshot(
            &mut self,
            request: grpc::Request<CreateSnapshotRequest>,
        ) -> Self::CreateSnapshotFuture;

        fn delete_snapshot(
            &mut self,
            request: grpc::Request<DeleteSnapshotRequest>,
        ) -> Self::DeleteSnapshotFuture;

        fn list_snapshots(
            &mut self,
            request: grpc::Request<ListSnapshotsRequest>,
        ) -> Self::ListSnapshotsFuture;

        fn controller_expand_volume(
            &mut self,
            request: grpc::Request<ControllerExpandVolumeRequest>,
        ) -> Self::ControllerExpandVolumeFuture;
    }

    #[derive(Debug, Clone)]
    pub struct ControllerServer<T> {
        controller: T,
    }

    impl<T> ControllerServer<T>
    where
        T: Controller,
    {
        pub fn new(controller: T) -> Self {
            Self { controller }
        }
    }

    impl<T> tower::Service<http::Request<grpc::BoxBody>> for ControllerServer<T>
    where
        T: Controller,
    {
        type Response = http::Response<controller::ResponseBody<T>>;
        type Error = grpc::Never;
        type Future = controller::ResponseFuture<T>;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            Ok(().into())
        }

        fn call(&mut self, request: http::Request<grpc::BoxBody>) -> Self::Future {
            use self::controller::Kind::*;

            match request.uri().path() {
                "/csi.v1.Controller/CreateVolume" => {
                    let service = controller::methods::CreateVolume(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: CreateVolume(response),
                    }
                }
                "/csi.v1.Controller/DeleteVolume" => {
                    let service = controller::methods::DeleteVolume(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: DeleteVolume(response),
                    }
                }
                "/csi.v1.Controller/ControllerPublishVolume" => {
                    let service =
                        controller::methods::ControllerPublishVolume(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: ControllerPublishVolume(response),
                    }
                }
                "/csi.v1.Controller/ControllerUnpublishVolume" => {
                    let service =
                        controller::methods::ControllerUnpublishVolume(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: ControllerUnpublishVolume(response),
                    }
                }
                "/csi.v1.Controller/ValidateVolumeCapabilities" => {
                    let service =
                        controller::methods::ValidateVolumeCapabilities(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: ValidateVolumeCapabilities(response),
                    }
                }
                "/csi.v1.Controller/ListVolumes" => {
                    let service = controller::methods::ListVolumes(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: ListVolumes(response),
                    }
                }
                "/csi.v1.Controller/GetCapacity" => {
                    let service = controller::methods::GetCapacity(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: GetCapacity(response),
                    }
                }
                "/csi.v1.Controller/ControllerGetCapabilities" => {
                    let service =
                        controller::methods::ControllerGetCapabilities(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: ControllerGetCapabilities(response),
                    }
                }
                "/csi.v1.Controller/CreateSnapshot" => {
                    let service = controller::methods::CreateSnapshot(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: CreateSnapshot(response),
                    }
                }
                "/csi.v1.Controller/DeleteSnapshot" => {
                    let service = controller::methods::DeleteSnapshot(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: DeleteSnapshot(response),
                    }
                }
                "/csi.v1.Controller/ListSnapshots" => {
                    let service = controller::methods::ListSnapshots(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: ListSnapshots(response),
                    }
                }
                "/csi.v1.Controller/ControllerExpandVolume" => {
                    let service =
                        controller::methods::ControllerExpandVolume(self.controller.clone());
                    let response = grpc::unary(service, request);
                    controller::ResponseFuture {
                        kind: ControllerExpandVolume(response),
                    }
                }
                _ => controller::ResponseFuture {
                    kind: __Generated__Unimplemented(grpc::unimplemented(format!(
                        "unknown service: {:?}",
                        request.uri().path()
                    ))),
                },
            }
        }
    }

    impl<T> tower::Service<()> for ControllerServer<T>
    where
        T: Controller,
    {
        type Response = Self;
        type Error = grpc::Never;
        type Future = futures::FutureResult<Self::Response, Self::Error>;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            Ok(futures::Async::Ready(()))
        }

        fn call(&mut self, _target: ()) -> Self::Future {
            futures::ok(self.clone())
        }
    }

    impl<T> tower::Service<http::Request<tower_hyper::Body>> for ControllerServer<T>
    where
        T: Controller,
    {
        type Response = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Response;
        type Error = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Error;
        type Future = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Future;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            tower::Service::<http::Request<grpc::BoxBody>>::poll_ready(self)
        }

        fn call(&mut self, request: http::Request<tower_hyper::Body>) -> Self::Future {
            let request = request.map(|b| grpc::BoxBody::map_from(b));
            tower::Service::<http::Request<grpc::BoxBody>>::call(self, request)
        }
    }

    pub mod controller {
        use super::super::{
            ControllerExpandVolumeRequest, ControllerGetCapabilitiesRequest,
            ControllerPublishVolumeRequest, ControllerUnpublishVolumeRequest,
            CreateSnapshotRequest, CreateVolumeRequest, DeleteSnapshotRequest, DeleteVolumeRequest,
            GetCapacityRequest, ListSnapshotsRequest, ListVolumesRequest,
            ValidateVolumeCapabilitiesRequest,
        };
        use super::Controller;
        use tower_grpc::codegen::server::*;

        pub struct ResponseFuture<T>
        where
            T: Controller,
        {
            pub(super) kind: Kind<
                // CreateVolume
                grpc::unary::ResponseFuture<
                    methods::CreateVolume<T>,
                    grpc::BoxBody,
                    CreateVolumeRequest,
                >,
                // DeleteVolume
                grpc::unary::ResponseFuture<
                    methods::DeleteVolume<T>,
                    grpc::BoxBody,
                    DeleteVolumeRequest,
                >,
                // ControllerPublishVolume
                grpc::unary::ResponseFuture<
                    methods::ControllerPublishVolume<T>,
                    grpc::BoxBody,
                    ControllerPublishVolumeRequest,
                >,
                // ControllerUnpublishVolume
                grpc::unary::ResponseFuture<
                    methods::ControllerUnpublishVolume<T>,
                    grpc::BoxBody,
                    ControllerUnpublishVolumeRequest,
                >,
                // ValidateVolumeCapabilities
                grpc::unary::ResponseFuture<
                    methods::ValidateVolumeCapabilities<T>,
                    grpc::BoxBody,
                    ValidateVolumeCapabilitiesRequest,
                >,
                // ListVolumes
                grpc::unary::ResponseFuture<
                    methods::ListVolumes<T>,
                    grpc::BoxBody,
                    ListVolumesRequest,
                >,
                // GetCapacity
                grpc::unary::ResponseFuture<
                    methods::GetCapacity<T>,
                    grpc::BoxBody,
                    GetCapacityRequest,
                >,
                // ControllerGetCapabilities
                grpc::unary::ResponseFuture<
                    methods::ControllerGetCapabilities<T>,
                    grpc::BoxBody,
                    ControllerGetCapabilitiesRequest,
                >,
                // CreateSnapshot
                grpc::unary::ResponseFuture<
                    methods::CreateSnapshot<T>,
                    grpc::BoxBody,
                    CreateSnapshotRequest,
                >,
                // DeleteSnapshot
                grpc::unary::ResponseFuture<
                    methods::DeleteSnapshot<T>,
                    grpc::BoxBody,
                    DeleteSnapshotRequest,
                >,
                // ListSnapshots
                grpc::unary::ResponseFuture<
                    methods::ListSnapshots<T>,
                    grpc::BoxBody,
                    ListSnapshotsRequest,
                >,
                // ControllerExpandVolume
                grpc::unary::ResponseFuture<
                    methods::ControllerExpandVolume<T>,
                    grpc::BoxBody,
                    ControllerExpandVolumeRequest,
                >,
                // A generated catch-all for unimplemented service calls
                grpc::unimplemented::ResponseFuture,
            >,
        }

        impl<T> futures::Future for ResponseFuture<T>
        where
            T: Controller,
        {
            type Item = http::Response<ResponseBody<T>>;
            type Error = grpc::Never;

            fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    CreateVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: CreateVolume(body),
                        });
                        Ok(response.into())
                    }
                    DeleteVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: DeleteVolume(body),
                        });
                        Ok(response.into())
                    }
                    ControllerPublishVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: ControllerPublishVolume(body),
                        });
                        Ok(response.into())
                    }
                    ControllerUnpublishVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: ControllerUnpublishVolume(body),
                        });
                        Ok(response.into())
                    }
                    ValidateVolumeCapabilities(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: ValidateVolumeCapabilities(body),
                        });
                        Ok(response.into())
                    }
                    ListVolumes(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: ListVolumes(body),
                        });
                        Ok(response.into())
                    }
                    GetCapacity(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: GetCapacity(body),
                        });
                        Ok(response.into())
                    }
                    ControllerGetCapabilities(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: ControllerGetCapabilities(body),
                        });
                        Ok(response.into())
                    }
                    CreateSnapshot(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: CreateSnapshot(body),
                        });
                        Ok(response.into())
                    }
                    DeleteSnapshot(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: DeleteSnapshot(body),
                        });
                        Ok(response.into())
                    }
                    ListSnapshots(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: ListSnapshots(body),
                        });
                        Ok(response.into())
                    }
                    ControllerExpandVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: ControllerExpandVolume(body),
                        });
                        Ok(response.into())
                    }
                    __Generated__Unimplemented(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: __Generated__Unimplemented(body),
                        });
                        Ok(response.into())
                    }
                }
            }
        }

        pub struct ResponseBody<T>
        where T: Controller,
        {
            pub(super) kind: Kind<
                // CreateVolume
                grpc::Encode<grpc::unary::Once<<methods::CreateVolume<T> as grpc::UnaryService<CreateVolumeRequest>>::Response>>,
                // DeleteVolume
                grpc::Encode<grpc::unary::Once<<methods::DeleteVolume<T> as grpc::UnaryService<DeleteVolumeRequest>>::Response>>,
                // ControllerPublishVolume
                grpc::Encode<grpc::unary::Once<<methods::ControllerPublishVolume<T> as grpc::UnaryService<ControllerPublishVolumeRequest>>::Response>>,
                // ControllerUnpublishVolume
                grpc::Encode<grpc::unary::Once<<methods::ControllerUnpublishVolume<T> as grpc::UnaryService<ControllerUnpublishVolumeRequest>>::Response>>,
                // ValidateVolumeCapabilities
                grpc::Encode<grpc::unary::Once<<methods::ValidateVolumeCapabilities<T> as grpc::UnaryService<ValidateVolumeCapabilitiesRequest>>::Response>>,
                // ListVolumes
                grpc::Encode<grpc::unary::Once<<methods::ListVolumes<T> as grpc::UnaryService<ListVolumesRequest>>::Response>>,
                // GetCapacity
                grpc::Encode<grpc::unary::Once<<methods::GetCapacity<T> as grpc::UnaryService<GetCapacityRequest>>::Response>>,
                // ControllerGetCapabilities
                grpc::Encode<grpc::unary::Once<<methods::ControllerGetCapabilities<T> as grpc::UnaryService<ControllerGetCapabilitiesRequest>>::Response>>,
                // CreateSnapshot
                grpc::Encode<grpc::unary::Once<<methods::CreateSnapshot<T> as grpc::UnaryService<CreateSnapshotRequest>>::Response>>,
                // DeleteSnapshot
                grpc::Encode<grpc::unary::Once<<methods::DeleteSnapshot<T> as grpc::UnaryService<DeleteSnapshotRequest>>::Response>>,
                // ListSnapshots
                grpc::Encode<grpc::unary::Once<<methods::ListSnapshots<T> as grpc::UnaryService<ListSnapshotsRequest>>::Response>>,
                // ControllerExpandVolume
                grpc::Encode<grpc::unary::Once<<methods::ControllerExpandVolume<T> as grpc::UnaryService<ControllerExpandVolumeRequest>>::Response>>,
                // A generated catch-all for unimplemented service calls
                (),
            >,
        }

        impl<T> tower::HttpBody for ResponseBody<T>
        where
            T: Controller,
        {
            type Data = <grpc::BoxBody as grpc::Body>::Data;
            type Error = grpc::Status;

            fn is_end_stream(&self) -> bool {
                use self::Kind::*;

                match self.kind {
                    CreateVolume(ref v) => v.is_end_stream(),
                    DeleteVolume(ref v) => v.is_end_stream(),
                    ControllerPublishVolume(ref v) => v.is_end_stream(),
                    ControllerUnpublishVolume(ref v) => v.is_end_stream(),
                    ValidateVolumeCapabilities(ref v) => v.is_end_stream(),
                    ListVolumes(ref v) => v.is_end_stream(),
                    GetCapacity(ref v) => v.is_end_stream(),
                    ControllerGetCapabilities(ref v) => v.is_end_stream(),
                    CreateSnapshot(ref v) => v.is_end_stream(),
                    DeleteSnapshot(ref v) => v.is_end_stream(),
                    ListSnapshots(ref v) => v.is_end_stream(),
                    ControllerExpandVolume(ref v) => v.is_end_stream(),
                    __Generated__Unimplemented(_) => true,
                }
            }

            fn poll_data(&mut self) -> futures::Poll<Option<Self::Data>, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    CreateVolume(ref mut v) => v.poll_data(),
                    DeleteVolume(ref mut v) => v.poll_data(),
                    ControllerPublishVolume(ref mut v) => v.poll_data(),
                    ControllerUnpublishVolume(ref mut v) => v.poll_data(),
                    ValidateVolumeCapabilities(ref mut v) => v.poll_data(),
                    ListVolumes(ref mut v) => v.poll_data(),
                    GetCapacity(ref mut v) => v.poll_data(),
                    ControllerGetCapabilities(ref mut v) => v.poll_data(),
                    CreateSnapshot(ref mut v) => v.poll_data(),
                    DeleteSnapshot(ref mut v) => v.poll_data(),
                    ListSnapshots(ref mut v) => v.poll_data(),
                    ControllerExpandVolume(ref mut v) => v.poll_data(),
                    __Generated__Unimplemented(_) => Ok(None.into()),
                }
            }

            fn poll_trailers(&mut self) -> futures::Poll<Option<http::HeaderMap>, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    CreateVolume(ref mut v) => v.poll_trailers(),
                    DeleteVolume(ref mut v) => v.poll_trailers(),
                    ControllerPublishVolume(ref mut v) => v.poll_trailers(),
                    ControllerUnpublishVolume(ref mut v) => v.poll_trailers(),
                    ValidateVolumeCapabilities(ref mut v) => v.poll_trailers(),
                    ListVolumes(ref mut v) => v.poll_trailers(),
                    GetCapacity(ref mut v) => v.poll_trailers(),
                    ControllerGetCapabilities(ref mut v) => v.poll_trailers(),
                    CreateSnapshot(ref mut v) => v.poll_trailers(),
                    DeleteSnapshot(ref mut v) => v.poll_trailers(),
                    ListSnapshots(ref mut v) => v.poll_trailers(),
                    ControllerExpandVolume(ref mut v) => v.poll_trailers(),
                    __Generated__Unimplemented(_) => Ok(None.into()),
                }
            }
        }

        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone)]
        pub(super) enum Kind<
            CreateVolume,
            DeleteVolume,
            ControllerPublishVolume,
            ControllerUnpublishVolume,
            ValidateVolumeCapabilities,
            ListVolumes,
            GetCapacity,
            ControllerGetCapabilities,
            CreateSnapshot,
            DeleteSnapshot,
            ListSnapshots,
            ControllerExpandVolume,
            __Generated__Unimplemented,
        > {
            CreateVolume(CreateVolume),
            DeleteVolume(DeleteVolume),
            ControllerPublishVolume(ControllerPublishVolume),
            ControllerUnpublishVolume(ControllerUnpublishVolume),
            ValidateVolumeCapabilities(ValidateVolumeCapabilities),
            ListVolumes(ListVolumes),
            GetCapacity(GetCapacity),
            ControllerGetCapabilities(ControllerGetCapabilities),
            CreateSnapshot(CreateSnapshot),
            DeleteSnapshot(DeleteSnapshot),
            ListSnapshots(ListSnapshots),
            ControllerExpandVolume(ControllerExpandVolume),
            __Generated__Unimplemented(__Generated__Unimplemented),
        }

        pub mod methods {
            use super::super::{
                Controller, ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
                ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
                ControllerPublishVolumeRequest, ControllerPublishVolumeResponse,
                ControllerUnpublishVolumeRequest, ControllerUnpublishVolumeResponse,
                CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest,
                CreateVolumeResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
                DeleteVolumeRequest, DeleteVolumeResponse, GetCapacityRequest, GetCapacityResponse,
                ListSnapshotsRequest, ListSnapshotsResponse, ListVolumesRequest,
                ListVolumesResponse, ValidateVolumeCapabilitiesRequest,
                ValidateVolumeCapabilitiesResponse,
            };
            use tower_grpc::codegen::server::*;

            pub struct CreateVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<CreateVolumeRequest>> for CreateVolume<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<CreateVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::CreateVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<CreateVolumeRequest>) -> Self::Future {
                    self.0.create_volume(request)
                }
            }

            pub struct DeleteVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<DeleteVolumeRequest>> for DeleteVolume<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<DeleteVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::DeleteVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<DeleteVolumeRequest>) -> Self::Future {
                    self.0.delete_volume(request)
                }
            }

            pub struct ControllerPublishVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<ControllerPublishVolumeRequest>> for ControllerPublishVolume<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<ControllerPublishVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::ControllerPublishVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<ControllerPublishVolumeRequest>,
                ) -> Self::Future {
                    self.0.controller_publish_volume(request)
                }
            }

            pub struct ControllerUnpublishVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<ControllerUnpublishVolumeRequest>>
                for ControllerUnpublishVolume<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<ControllerUnpublishVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::ControllerUnpublishVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<ControllerUnpublishVolumeRequest>,
                ) -> Self::Future {
                    self.0.controller_unpublish_volume(request)
                }
            }

            pub struct ValidateVolumeCapabilities<T>(pub T);

            impl<T> tower::Service<grpc::Request<ValidateVolumeCapabilitiesRequest>>
                for ValidateVolumeCapabilities<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<ValidateVolumeCapabilitiesResponse>;
                type Error = grpc::Status;
                type Future = T::ValidateVolumeCapabilitiesFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<ValidateVolumeCapabilitiesRequest>,
                ) -> Self::Future {
                    self.0.validate_volume_capabilities(request)
                }
            }

            pub struct ListVolumes<T>(pub T);

            impl<T> tower::Service<grpc::Request<ListVolumesRequest>> for ListVolumes<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<ListVolumesResponse>;
                type Error = grpc::Status;
                type Future = T::ListVolumesFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<ListVolumesRequest>) -> Self::Future {
                    self.0.list_volumes(request)
                }
            }

            pub struct GetCapacity<T>(pub T);

            impl<T> tower::Service<grpc::Request<GetCapacityRequest>> for GetCapacity<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<GetCapacityResponse>;
                type Error = grpc::Status;
                type Future = T::GetCapacityFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<GetCapacityRequest>) -> Self::Future {
                    self.0.get_capacity(request)
                }
            }

            pub struct ControllerGetCapabilities<T>(pub T);

            impl<T> tower::Service<grpc::Request<ControllerGetCapabilitiesRequest>>
                for ControllerGetCapabilities<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<ControllerGetCapabilitiesResponse>;
                type Error = grpc::Status;
                type Future = T::ControllerGetCapabilitiesFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<ControllerGetCapabilitiesRequest>,
                ) -> Self::Future {
                    self.0.controller_get_capabilities(request)
                }
            }

            pub struct CreateSnapshot<T>(pub T);

            impl<T> tower::Service<grpc::Request<CreateSnapshotRequest>> for CreateSnapshot<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<CreateSnapshotResponse>;
                type Error = grpc::Status;
                type Future = T::CreateSnapshotFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<CreateSnapshotRequest>) -> Self::Future {
                    self.0.create_snapshot(request)
                }
            }

            pub struct DeleteSnapshot<T>(pub T);

            impl<T> tower::Service<grpc::Request<DeleteSnapshotRequest>> for DeleteSnapshot<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<DeleteSnapshotResponse>;
                type Error = grpc::Status;
                type Future = T::DeleteSnapshotFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<DeleteSnapshotRequest>) -> Self::Future {
                    self.0.delete_snapshot(request)
                }
            }

            pub struct ListSnapshots<T>(pub T);

            impl<T> tower::Service<grpc::Request<ListSnapshotsRequest>> for ListSnapshots<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<ListSnapshotsResponse>;
                type Error = grpc::Status;
                type Future = T::ListSnapshotsFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<ListSnapshotsRequest>) -> Self::Future {
                    self.0.list_snapshots(request)
                }
            }

            pub struct ControllerExpandVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<ControllerExpandVolumeRequest>> for ControllerExpandVolume<T>
            where
                T: Controller,
            {
                type Response = grpc::Response<ControllerExpandVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::ControllerExpandVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<ControllerExpandVolumeRequest>,
                ) -> Self::Future {
                    self.0.controller_expand_volume(request)
                }
            }
        }
    }

    // Redefine the try_ready macro so that it doesn't need to be explicitly
    // imported by the user of this generated code.
    macro_rules! try_ready {
        ($e:expr) => {
            match $e {
                Ok(futures::Async::Ready(t)) => t,
                Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                Err(e) => return Err(From::from(e)),
            }
        };
    }

    pub trait Node: Clone {
        type NodeStageVolumeFuture: futures::Future<
            Item = grpc::Response<NodeStageVolumeResponse>,
            Error = grpc::Status,
        >;
        type NodeUnstageVolumeFuture: futures::Future<
            Item = grpc::Response<NodeUnstageVolumeResponse>,
            Error = grpc::Status,
        >;
        type NodeListVolumeFuture: futures::Future<
          Item = grpc::Response<NodeListVolumesResponse>,
            Error = grpc::Status,
        >;
        type NodePublishVolumeFuture: futures::Future<
            Item = grpc::Response<NodePublishVolumeResponse>,
            Error = grpc::Status,
        >;
        type NodeUnpublishVolumeFuture: futures::Future<
            Item = grpc::Response<NodeUnpublishVolumeResponse>,
            Error = grpc::Status,
        >;
        type NodeGetVolumeStatsFuture: futures::Future<
            Item = grpc::Response<NodeGetVolumeStatsResponse>,
            Error = grpc::Status,
        >;
        type NodeExpandVolumeFuture: futures::Future<
            Item = grpc::Response<NodeExpandVolumeResponse>,
            Error = grpc::Status,
        >;
        type NodeGetCapabilitiesFuture: futures::Future<
            Item = grpc::Response<NodeGetCapabilitiesResponse>,
            Error = grpc::Status,
        >;
        type NodeGetInfoFuture: futures::Future<
            Item = grpc::Response<NodeGetInfoResponse>,
            Error = grpc::Status,
        >;

        fn node_stage_volume(
            &mut self,
            request: grpc::Request<NodeStageVolumeRequest>,
        ) -> Self::NodeStageVolumeFuture;

        fn node_unstage_volume(
            &mut self,
            request: grpc::Request<NodeUnstageVolumeRequest>,
        ) -> Self::NodeUnstageVolumeFuture;

        fn node_list_volumes(
            &mut self,
            request: grpc::Request<NodeListVolumesRequest>,
        ) -> Self::NodeListVolumeFuture;

        fn node_publish_volume(
            &mut self,
            request: grpc::Request<NodePublishVolumeRequest>,
        ) -> Self::NodePublishVolumeFuture;

        fn node_unpublish_volume(
            &mut self,
            request: grpc::Request<NodeUnpublishVolumeRequest>,
        ) -> Self::NodeUnpublishVolumeFuture;

        fn node_get_volume_stats(
            &mut self,
            request: grpc::Request<NodeGetVolumeStatsRequest>,
        ) -> Self::NodeGetVolumeStatsFuture;

        fn node_expand_volume(
            &mut self,
            request: grpc::Request<NodeExpandVolumeRequest>,
        ) -> Self::NodeExpandVolumeFuture;

        fn node_get_capabilities(
            &mut self,
            request: grpc::Request<NodeGetCapabilitiesRequest>,
        ) -> Self::NodeGetCapabilitiesFuture;

        fn node_get_info(
            &mut self,
            request: grpc::Request<NodeGetInfoRequest>,
        ) -> Self::NodeGetInfoFuture;
    }

    #[derive(Debug, Clone)]
    pub struct NodeServer<T> {
        node: T,
    }

    impl<T> NodeServer<T>
    where
        T: Node,
    {
        pub fn new(node: T) -> Self {
            Self { node }
        }
    }

    impl<T> tower::Service<http::Request<grpc::BoxBody>> for NodeServer<T>
    where
        T: Node,
    {
        type Response = http::Response<node::ResponseBody<T>>;
        type Error = grpc::Never;
        type Future = node::ResponseFuture<T>;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            Ok(().into())
        }

        fn call(&mut self, request: http::Request<grpc::BoxBody>) -> Self::Future {
            use self::node::Kind::*;

            match request.uri().path() {
                "/csi.v1.Node/NodeStageVolume" => {
                    let service = node::methods::NodeStageVolume(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodeStageVolume(response),
                    }
                }
                "/csi.v1.Node/NodeUnstageVolume" => {
                    let service = node::methods::NodeUnstageVolume(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodeUnstageVolume(response),
                    }
                }
                "/csi.v1.Node/NodeListVolumes" => {
                    let service = node::methods::NodeListVolumes(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodeListVolumes(response),
                    }
                }
                "/csi.v1.Node/NodePublishVolume" => {
                    let service = node::methods::NodePublishVolume(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodePublishVolume(response),
                    }
                }
                "/csi.v1.Node/NodeUnpublishVolume" => {
                    let service = node::methods::NodeUnpublishVolume(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodeUnpublishVolume(response),
                    }
                }
                "/csi.v1.Node/NodeGetVolumeStats" => {
                    let service = node::methods::NodeGetVolumeStats(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodeGetVolumeStats(response),
                    }
                }
                "/csi.v1.Node/NodeExpandVolume" => {
                    let service = node::methods::NodeExpandVolume(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodeExpandVolume(response),
                    }
                }
                "/csi.v1.Node/NodeGetCapabilities" => {
                    let service = node::methods::NodeGetCapabilities(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodeGetCapabilities(response),
                    }
                }
                "/csi.v1.Node/NodeGetInfo" => {
                    let service = node::methods::NodeGetInfo(self.node.clone());
                    let response = grpc::unary(service, request);
                    node::ResponseFuture {
                        kind: NodeGetInfo(response),
                    }
                }
                _ => node::ResponseFuture {
                    kind: __Generated__Unimplemented(grpc::unimplemented(format!(
                        "unknown service: {:?}",
                        request.uri().path()
                    ))),
                },
            }
        }
    }

    impl<T> tower::Service<()> for NodeServer<T>
    where
        T: Node,
    {
        type Response = Self;
        type Error = grpc::Never;
        type Future = futures::FutureResult<Self::Response, Self::Error>;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            Ok(futures::Async::Ready(()))
        }

        fn call(&mut self, _target: ()) -> Self::Future {
            futures::ok(self.clone())
        }
    }

    impl<T> tower::Service<http::Request<tower_hyper::Body>> for NodeServer<T>
    where
        T: Node,
    {
        type Response = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Response;
        type Error = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Error;
        type Future = <Self as tower::Service<http::Request<grpc::BoxBody>>>::Future;

        fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
            tower::Service::<http::Request<grpc::BoxBody>>::poll_ready(self)
        }

        fn call(&mut self, request: http::Request<tower_hyper::Body>) -> Self::Future {
            let request = request.map(|b| grpc::BoxBody::map_from(b));
            tower::Service::<http::Request<grpc::BoxBody>>::call(self, request)
        }
    }

    pub mod node {
        use super::super::{
            NodeExpandVolumeRequest, NodeGetCapabilitiesRequest, NodeGetInfoRequest,
            NodeGetVolumeStatsRequest, NodePublishVolumeRequest, NodeStageVolumeRequest,
            NodeListVolumesRequest,
            NodeUnpublishVolumeRequest, NodeUnstageVolumeRequest,
        };
        use super::Node;
        use tower_grpc::codegen::server::*;

        pub struct ResponseFuture<T>
        where
            T: Node,
        {
            pub(super) kind: Kind<
                // NodeStageVolume
                grpc::unary::ResponseFuture<
                    methods::NodeStageVolume<T>,
                    grpc::BoxBody,
                    NodeStageVolumeRequest,
                >,
                // NodeUnstageVolume
                grpc::unary::ResponseFuture<
                    methods::NodeUnstageVolume<T>,
                    grpc::BoxBody,
                    NodeUnstageVolumeRequest,
                >,
                // NodeListVolumes
                grpc::unary::ResponseFuture<
                    methods::NodeListVolumes<T>,
                    grpc::BoxBody,
                    NodeListVolumesRequest,
                >,
                // NodePublishVolume
                grpc::unary::ResponseFuture<
                    methods::NodePublishVolume<T>,
                    grpc::BoxBody,
                    NodePublishVolumeRequest,
                >,
                // NodeUnpublishVolume
                grpc::unary::ResponseFuture<
                    methods::NodeUnpublishVolume<T>,
                    grpc::BoxBody,
                    NodeUnpublishVolumeRequest,
                >,
                // NodeGetVolumeStats
                grpc::unary::ResponseFuture<
                    methods::NodeGetVolumeStats<T>,
                    grpc::BoxBody,
                    NodeGetVolumeStatsRequest,
                >,
                // NodeExpandVolume
                grpc::unary::ResponseFuture<
                    methods::NodeExpandVolume<T>,
                    grpc::BoxBody,
                    NodeExpandVolumeRequest,
                >,
                // NodeGetCapabilities
                grpc::unary::ResponseFuture<
                    methods::NodeGetCapabilities<T>,
                    grpc::BoxBody,
                    NodeGetCapabilitiesRequest,
                >,
                // NodeGetInfo
                grpc::unary::ResponseFuture<
                    methods::NodeGetInfo<T>,
                    grpc::BoxBody,
                    NodeGetInfoRequest,
                >,
                // A generated catch-all for unimplemented service calls
                grpc::unimplemented::ResponseFuture,
            >,
        }

        impl<T> futures::Future for ResponseFuture<T>
        where
            T: Node,
        {
            type Item = http::Response<ResponseBody<T>>;
            type Error = grpc::Never;

            fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    NodeStageVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodeStageVolume(body),
                        });
                        Ok(response.into())
                    }
                    NodeUnstageVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodeUnstageVolume(body),
                        });
                        Ok(response.into())
                    }
                    NodeListVolumes(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodeListVolumes(body),
                        });
                        Ok(response.into())
                    }
                    NodePublishVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodePublishVolume(body),
                        });
                        Ok(response.into())
                    }
                    NodeUnpublishVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodeUnpublishVolume(body),
                        });
                        Ok(response.into())
                    }
                    NodeGetVolumeStats(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodeGetVolumeStats(body),
                        });
                        Ok(response.into())
                    }
                    NodeExpandVolume(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodeExpandVolume(body),
                        });
                        Ok(response.into())
                    }
                    NodeGetCapabilities(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodeGetCapabilities(body),
                        });
                        Ok(response.into())
                    }
                    NodeGetInfo(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: NodeGetInfo(body),
                        });
                        Ok(response.into())
                    }
                    __Generated__Unimplemented(ref mut fut) => {
                        let response = try_ready!(fut.poll());
                        let response = response.map(|body| ResponseBody {
                            kind: __Generated__Unimplemented(body),
                        });
                        Ok(response.into())
                    }
                }
            }
        }

        pub struct ResponseBody<T>
        where T: Node,
        {
            pub(super) kind: Kind<
                // NodeStageVolume
                grpc::Encode<grpc::unary::Once<<methods::NodeStageVolume<T> as grpc::UnaryService<NodeStageVolumeRequest>>::Response>>,
                // NodeUnstageVolume
                grpc::Encode<grpc::unary::Once<<methods::NodeUnstageVolume<T> as grpc::UnaryService<NodeUnstageVolumeRequest>>::Response>>,
                // Nodelist volumes
                grpc::Encode<grpc::unary::Once<<methods::NodeListVolumes<T> as grpc::UnaryService<NodeListVolumesRequest>>::Response>>,
                // NodePublishVolume
                grpc::Encode<grpc::unary::Once<<methods::NodePublishVolume<T> as grpc::UnaryService<NodePublishVolumeRequest>>::Response>>,
                // NodeUnpublishVolume
                grpc::Encode<grpc::unary::Once<<methods::NodeUnpublishVolume<T> as grpc::UnaryService<NodeUnpublishVolumeRequest>>::Response>>,
                // NodeGetVolumeStats
                grpc::Encode<grpc::unary::Once<<methods::NodeGetVolumeStats<T> as grpc::UnaryService<NodeGetVolumeStatsRequest>>::Response>>,
                // NodeExpandVolume
                grpc::Encode<grpc::unary::Once<<methods::NodeExpandVolume<T> as grpc::UnaryService<NodeExpandVolumeRequest>>::Response>>,
                // NodeGetCapabilities
                grpc::Encode<grpc::unary::Once<<methods::NodeGetCapabilities<T> as grpc::UnaryService<NodeGetCapabilitiesRequest>>::Response>>,
                // NodeGetInfo
                grpc::Encode<grpc::unary::Once<<methods::NodeGetInfo<T> as grpc::UnaryService<NodeGetInfoRequest>>::Response>>,
                // A generated catch-all for unimplemented service calls
                (),
            >,
        }

        impl<T> tower::HttpBody for ResponseBody<T>
        where
            T: Node,
        {
            type Data = <grpc::BoxBody as grpc::Body>::Data;
            type Error = grpc::Status;

            fn is_end_stream(&self) -> bool {
                use self::Kind::*;

                match self.kind {
                    NodeStageVolume(ref v) => v.is_end_stream(),
                    NodeUnstageVolume(ref v) => v.is_end_stream(),
                    NodeListVolumes(ref v) => v.is_end_stream(),
                    NodePublishVolume(ref v) => v.is_end_stream(),
                    NodeUnpublishVolume(ref v) => v.is_end_stream(),
                    NodeGetVolumeStats(ref v) => v.is_end_stream(),
                    NodeExpandVolume(ref v) => v.is_end_stream(),
                    NodeGetCapabilities(ref v) => v.is_end_stream(),
                    NodeGetInfo(ref v) => v.is_end_stream(),
                    __Generated__Unimplemented(_) => true,
                }
            }

            fn poll_data(&mut self) -> futures::Poll<Option<Self::Data>, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    NodeStageVolume(ref mut v) => v.poll_data(),
                    NodeUnstageVolume(ref mut v) => v.poll_data(),
                    NodeListVolumes(ref mut v) => v.poll_data(),
                    NodePublishVolume(ref mut v) => v.poll_data(),
                    NodeUnpublishVolume(ref mut v) => v.poll_data(),
                    NodeGetVolumeStats(ref mut v) => v.poll_data(),
                    NodeExpandVolume(ref mut v) => v.poll_data(),
                    NodeGetCapabilities(ref mut v) => v.poll_data(),
                    NodeGetInfo(ref mut v) => v.poll_data(),
                    __Generated__Unimplemented(_) => Ok(None.into()),
                }
            }

            fn poll_trailers(&mut self) -> futures::Poll<Option<http::HeaderMap>, Self::Error> {
                use self::Kind::*;

                match self.kind {
                    NodeStageVolume(ref mut v) => v.poll_trailers(),
                    NodeUnstageVolume(ref mut v) => v.poll_trailers(),
                    NodeListVolumes(ref mut v) => v.poll_trailers(),
                    NodePublishVolume(ref mut v) => v.poll_trailers(),
                    NodeUnpublishVolume(ref mut v) => v.poll_trailers(),
                    NodeGetVolumeStats(ref mut v) => v.poll_trailers(),
                    NodeExpandVolume(ref mut v) => v.poll_trailers(),
                    NodeGetCapabilities(ref mut v) => v.poll_trailers(),
                    NodeGetInfo(ref mut v) => v.poll_trailers(),
                    __Generated__Unimplemented(_) => Ok(None.into()),
                }
            }
        }

        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone)]
        pub(super) enum Kind<
            NodeStageVolume,
            NodeUnstageVolume,
            NodeListVolumes,
            NodePublishVolume,
            NodeUnpublishVolume,
            NodeGetVolumeStats,
            NodeExpandVolume,
            NodeGetCapabilities,
            NodeGetInfo,
            __Generated__Unimplemented,
        > {
            NodeStageVolume(NodeStageVolume),
            NodeUnstageVolume(NodeUnstageVolume),
            NodeListVolumes(NodeListVolumes),
            NodePublishVolume(NodePublishVolume),
            NodeUnpublishVolume(NodeUnpublishVolume),
            NodeGetVolumeStats(NodeGetVolumeStats),
            NodeExpandVolume(NodeExpandVolume),
            NodeGetCapabilities(NodeGetCapabilities),
            NodeGetInfo(NodeGetInfo),
            __Generated__Unimplemented(__Generated__Unimplemented),
        }

        pub mod methods {
            use super::super::{
                Node, NodeExpandVolumeRequest, NodeExpandVolumeResponse,
                NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse, NodeGetInfoRequest,
                NodeGetInfoResponse, NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse,
                NodePublishVolumeRequest, NodePublishVolumeResponse, NodeStageVolumeRequest,
                NodeStageVolumeResponse, NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse,
                NodeUnstageVolumeRequest, NodeUnstageVolumeResponse,
                NodeListVolumesRequest, NodeListVolumesResponse,
            };
            use tower_grpc::codegen::server::*;

            pub struct NodeStageVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodeStageVolumeRequest>> for NodeStageVolume<T>
            where
                T: Node,
            {
                type Response = grpc::Response<NodeStageVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::NodeStageVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<NodeStageVolumeRequest>) -> Self::Future {
                    self.0.node_stage_volume(request)
                }
            }

            pub struct NodeUnstageVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodeUnstageVolumeRequest>> for NodeUnstageVolume<T>
            where
                T: Node,
            {
                type Response = grpc::Response<NodeUnstageVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::NodeUnstageVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<NodeUnstageVolumeRequest>,
                ) -> Self::Future {
                    self.0.node_unstage_volume(request)
                }
            }

            // additional
            pub struct NodeListVolumes<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodeListVolumesRequest>> for NodeListVolumes<T>
                where
                    T: Node,
            {
                type Response = grpc::Response<NodeListVolumesResponse>;
                type Error = grpc::Status;
                type Future = T::NodeListVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<NodeListVolumesRequest>,
                ) -> Self::Future {
                    self.0.node_list_volumes(request)
                }
            }

            pub struct NodePublishVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodePublishVolumeRequest>> for NodePublishVolume<T>
            where
                T: Node,
            {
                type Response = grpc::Response<NodePublishVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::NodePublishVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<NodePublishVolumeRequest>,
                ) -> Self::Future {
                    self.0.node_publish_volume(request)
                }
            }

            pub struct NodeUnpublishVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodeUnpublishVolumeRequest>> for NodeUnpublishVolume<T>
            where
                T: Node,
            {
                type Response = grpc::Response<NodeUnpublishVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::NodeUnpublishVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<NodeUnpublishVolumeRequest>,
                ) -> Self::Future {
                    self.0.node_unpublish_volume(request)
                }
            }

            pub struct NodeGetVolumeStats<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodeGetVolumeStatsRequest>> for NodeGetVolumeStats<T>
            where
                T: Node,
            {
                type Response = grpc::Response<NodeGetVolumeStatsResponse>;
                type Error = grpc::Status;
                type Future = T::NodeGetVolumeStatsFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<NodeGetVolumeStatsRequest>,
                ) -> Self::Future {
                    self.0.node_get_volume_stats(request)
                }
            }

            pub struct NodeExpandVolume<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodeExpandVolumeRequest>> for NodeExpandVolume<T>
            where
                T: Node,
            {
                type Response = grpc::Response<NodeExpandVolumeResponse>;
                type Error = grpc::Status;
                type Future = T::NodeExpandVolumeFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<NodeExpandVolumeRequest>,
                ) -> Self::Future {
                    self.0.node_expand_volume(request)
                }
            }

            pub struct NodeGetCapabilities<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodeGetCapabilitiesRequest>> for NodeGetCapabilities<T>
            where
                T: Node,
            {
                type Response = grpc::Response<NodeGetCapabilitiesResponse>;
                type Error = grpc::Status;
                type Future = T::NodeGetCapabilitiesFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(
                    &mut self,
                    request: grpc::Request<NodeGetCapabilitiesRequest>,
                ) -> Self::Future {
                    self.0.node_get_capabilities(request)
                }
            }

            pub struct NodeGetInfo<T>(pub T);

            impl<T> tower::Service<grpc::Request<NodeGetInfoRequest>> for NodeGetInfo<T>
            where
                T: Node,
            {
                type Response = grpc::Response<NodeGetInfoResponse>;
                type Error = grpc::Status;
                type Future = T::NodeGetInfoFuture;

                fn poll_ready(&mut self) -> futures::Poll<(), Self::Error> {
                    Ok(futures::Async::Ready(()))
                }

                fn call(&mut self, request: grpc::Request<NodeGetInfoRequest>) -> Self::Future {
                    self.0.node_get_info(request)
                }
            }
        }
    }
}
