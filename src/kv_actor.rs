use std::time::Duration;

use kameo::{mailbox::unbounded::UnboundedMailbox, message::{Context, Message}, Actor};

struct KVActor {

}

impl Default for KVActor {
    fn default() -> Self {
        Self {}
    }
}

impl Actor for KVActor {
    type Mailbox = UnboundedMailbox<Self>;
    fn name() -> &'static str {
        "KVActor"
    }

    fn on_start(
            &mut self,
            actor_ref: kameo::actor::ActorRef<Self>,
        ) -> impl std::future::Future<Output = Result<(), kameo::error::BoxError>> + Send {
        async move {
           println!("KVActor started");
            Ok(())
        }
    }

    fn on_stop(
            &mut self,
            actor_ref: kameo::actor::WeakActorRef<Self>,
            reason: kameo::error::ActorStopReason,
        ) -> impl std::future::Future<Output = Result<(), kameo::error::BoxError>> + Send {
        async move {
            println!("KVActor stopped");
            Ok(())
        }
    }
}

enum KVMessage {
    WalkDir,
    WalkDirReply(Result<Vec<String>>),
}

impl Message<KVMessage> for KVMessage {
    type Reply = Self;
    async fn handle(
            &mut self,
            msg: KVMessage,
            ctx: Context<'_, Self, Self::Reply>,
        ) -> Self::Reply {
        match msg {
            KVMessage::WalkDir => {
                ctx.reply(KVMessage::WalkDirReply(Ok(vec!["test".to_string()]))).await;
            }
            KVMessage::WalkDirReply(res) => {
                println!("WalkDirReply: {:?}", res);
            }
        }
    }
}   

#[tokio::test]
async fn test_kv_actor() {
    env_logger::Env::default().filter_or("RUST_LOG", "info");
    let my_actor_ref = kameo::spawn(KVActor::default());
    my_actor_ref.kill();
    tokio::time::sleep(Duration::from_millis(200)).await;
}