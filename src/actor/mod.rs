use std::time::Duration;

use kameo::{
    actor::{ActorPool, BroadcastMsg, WorkerMsg},
    message::{Context, Message},
    request::MessageSend,
    Actor,
};

#[derive(Actor, Default)]
struct MyActor;

struct PrintActorID;

impl Message<PrintActorID> for MyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PrintActorID,
        ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        println!("{}", ctx.actor_ref().id());
    }
}

#[derive(Clone)]
struct ForceStop;

impl Message<ForceStop> for MyActor {
    type Reply = ();

    async fn handle(&mut self, _: ForceStop, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        ctx.actor_ref().kill();
        ctx.actor_ref().wait_for_stop().await;
    }
}

#[tokio::test]
async fn t_actor() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter("warn".parse::<EnvFilter>().unwrap())
        .without_time()
        .with_target(false)
        .init();

    let pool = kameo::spawn(ActorPool::new(5, || kameo::spawn(MyActor)));

    // Print IDs from 0..=4
    for _ in 0..5 {
        pool.ask(WorkerMsg(PrintActorID)).send().await?;
    }

    pool.ask(BroadcastMsg(ForceStop)).send().await?;
    // tokio::time::sleep(Duration::from_millis(200)).await;

    println!("Restared all workers");

    // new IDs from 6..=10
    for _ in 0..5 {
        pool.ask(WorkerMsg(PrintActorID)).send().await?;
    }

    Ok(())
}
