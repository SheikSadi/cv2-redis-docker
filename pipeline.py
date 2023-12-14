import os
from .cache import Redis
from .manage import extractInSteps, manager, makeVideoInSteps, findInputCodec
from multiprocessing import Process
from pathlib import Path


def startPipeline(
    processImage,
    kwargs_processImage,
    input_video_path,
    output_dir,
    frames_per_iteration,
    n_processes,
    redis_host,
    redis_port,
    save_frames,
    temp_dir,
    early_stopping,
):
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    if isinstance(temp_dir, str):
        temp_dir = Path(temp_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    redis = Redis(redis_host, redis_port)

    input_fname_without_ext = ".".join(os.path.basename(input_video_path).split(".")[:-1])
    output_video_path = output_dir / f"[output] {input_fname_without_ext}.mp4"

    input_codec = findInputCodec(input_video_path)

    writeToVideo = makeVideoInSteps(
        output_video_path, frames_per_iteration, redis, output_codec=input_codec
    )
    extractFrame = extractInSteps(
        input_video_path, frames_per_iteration, early_stopping, redis
    )

    n_iterations = 0
    while 1:
        try:
            first_frame, last_frame, capture = next(extractFrame)
        except StopIteration:
            print("All the frames have been extracted.")
        except Exception as e:
            capture.release()
            print(f"Error occured during extraction: {e}")
        else:
            children = [
                Process(
                    target=manager,
                    args=(
                        processImage,
                        kwargs_processImage,
                        offset,
                        n_processes,
                        first_frame,
                        last_frame,
                        save_frames,
                        temp_dir,
                        redis,
                    ),
                    daemon=True,
                )
                for offset in range(n_processes)
            ]   
            for child in children:
                child.start()
                print(f"Child process started: {child.name} (pid={child.pid})")
            for child in children:
                child.join()
                print(f"Child process joined: {child.name} (pid={child.pid})")
        finally:
            try:
                next(writeToVideo)
            except StopIteration as stopped:
                print(f"The video writer released: {output_video_path}")
                break
            else:
                n_iterations += 1
                # IMPORTTANT
                print(
                    f"Iteration: #{n_iterations:02} complete. Number of frames processed: {n_iterations * frames_per_iteration}."
                )
                redis.flushall()
                print("Flushed the Cache!")
