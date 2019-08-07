use crate::csi::{
    server, NodeExpandVolumeRequest, NodeExpandVolumeResponse, NodeGetCapabilitiesRequest,
    NodeGetCapabilitiesResponse, NodeGetInfoRequest, NodeGetInfoResponse,
    NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse, NodePublishVolumeRequest,
    NodePublishVolumeResponse, NodeStageVolumeRequest, NodeStageVolumeResponse,
    NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse, NodeUnstageVolumeRequest,
    NodeUnstageVolumeResponse, NodeListVolumesResponse,NodeListVolumesRequest,
};
use futures::future::{FutureResult, err};
use futures::Future;
use tower_grpc::{Code, Request, Response, Status};
use tower_grpc::codegen::server::futures::ok;
use libzfs_rs::zfs::LibZfs;
use std::fmt::Error;

/// our main structure
#[derive(Clone, Debug, Default)]
pub struct CsiNode {
    /// name of this node
    name: String,
    socket: String,
}

impl CsiNode {
    pub fn new() -> Self {
        Self::default()
    }
}

impl server::Node for CsiNode {
    type NodeStageVolumeFuture =
        Box<dyn Future<Item = Response<NodeStageVolumeResponse>, Error = Status> + Send>;
    type NodeUnstageVolumeFuture =
        Box<dyn Future<Item = Response<NodeUnstageVolumeResponse>, Error = Status> + Send>;
    type NodeListVolumeFuture =
        Box<dyn Future<Item = Response<NodeListVolumesResponse>, Error = Status> + Send>;
    type NodePublishVolumeFuture =
        Box<dyn Future<Item = Response<NodePublishVolumeResponse>, Error = Status> + Send>;
    type NodeUnpublishVolumeFuture =
        Box<dyn Future<Item = Response<NodeUnpublishVolumeResponse>, Error = Status> + Send>;
    type NodeGetVolumeStatsFuture =
        Box<dyn Future<Item = Response<NodeGetVolumeStatsResponse>, Error = Status> + Send>;
    type NodeExpandVolumeFuture = FutureResult<Response<NodeExpandVolumeResponse>, Status>;
    type NodeGetCapabilitiesFuture = FutureResult<Response<NodeGetCapabilitiesResponse>, Status>;
    type NodeGetInfoFuture = FutureResult<Response<NodeGetInfoResponse>, Status>;

    fn node_stage_volume(
        &mut self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Self::NodeStageVolumeFuture {

        // TODO: with grpc, try it out. Also get snapshot done here

        let msg = request.into_inner();

        // get data set name and pool name from the message
        let ds_name = msg.volume_context.get("DataSetName").unwrap();

        // get zfs handle
        let zfs_handle = LibZfs::new();
        if zfs_handle.is_some() {
            let zfs = zfs_handle.unwrap();

            // create data set with volume id as the name
            let fs = zfs.create_filesystem(ds_name.as_str());

            if fs.is_ok() {
                Box::new(ok(Response::new(NodeStageVolumeResponse {})))
            } else {
                Box::new(err(Status::new(Code::Internal, "error in creating dataset")))
            }

        } else {
            Box::new(err(Status::new(Code::Internal, "error in zfs handle")))
        }
    }

    fn node_unstage_volume(
        &mut self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Self::NodeUnstageVolumeFuture {
        let msg = request.into_inner();

        // get data set name and pool name from the message
        let ds_name = msg.staging_target_path;

        // get zfs handle
        let zfs_handle = LibZfs::new();
        if zfs_handle.is_some() {
            let zfs = zfs_handle.unwrap();

            // create data set with volume id as the name
            let fs = zfs.destroy_filesystem(ds_name.as_str());

            if fs.is_ok() {
                Box::new(ok(Response::new(NodeUnstageVolumeResponse {})))
            } else {
                Box::new(err(Status::new(Code::Internal, "error in deleting dataset")))
            }

        } else {
            Box::new(err(Status::new(Code::Internal, "error in zfs handle")))
        }
    }

    fn node_publish_volume(
        &mut self,
        _request: Request<NodePublishVolumeRequest>,
    ) -> Self::NodePublishVolumeFuture {
        unimplemented!()
    }

    fn node_unpublish_volume(
        &mut self,
        _request: Request<NodeUnpublishVolumeRequest>,
    ) -> Self::NodeUnpublishVolumeFuture {
        unimplemented!()
    }

    fn node_list_volumes(
        &mut self,
        request: Request<NodeListVolumesRequest>,
    ) -> Self::NodeListVolumeFuture {

        // the pool/dataset on which to iterate
        let pool_name = "pool";

        let zfs_handle = LibZfs::new();

        //if zfs_handle.is_some() {
        let mut zfs = zfs_handle.unwrap();

        let list = zfs.iter_root(pool_name);

        Box::new(ok(Response::new(NodeListVolumesResponse{
            vol_names: list.to_vec(),
        })))

    }

    fn node_get_volume_stats(
        &mut self,
        _request: Request<NodeGetVolumeStatsRequest>,
    ) -> Self::NodeGetVolumeStatsFuture {
        unimplemented!()
    }

    fn node_expand_volume(
        &mut self,
        _request: Request<NodeExpandVolumeRequest>,
    ) -> Self::NodeExpandVolumeFuture {
        unimplemented!()
    }

    fn node_get_capabilities(
        &mut self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Self::NodeGetCapabilitiesFuture {
        unimplemented!()
    }

    fn node_get_info(&mut self, _request: Request<NodeGetInfoRequest>) -> Self::NodeGetInfoFuture {
        unimplemented!()
    }
}
