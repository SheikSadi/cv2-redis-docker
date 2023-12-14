import cv2
from . import const
import os


def findInputCodec(input_file):
    cap = cv2.VideoCapture(input_file)
    codec = cap.get(cv2.CAP_PROP_FOURCC)
    cap.release()
    return int(codec)


def manager(
    processImage, kwargs_processImage, offset, n_processes, first_frame, last_frame, save_frames, temp_dir, rds
):
    for frame_id in range(first_frame, last_frame):
        if offset == (frame_id % n_processes):
            frame = rds.getArray(frame_id)
            resultImg = processImage(img=frame, **kwargs_processImage)
            rds.setArray(frame_id, resultImg)
            print(f"frame #{frame_id:04} has been processed.")
            if save_frames:
                cv2.imwrite(f"{temp_dir}/{frame_id}.jpg", resultImg)



def extractInSteps(input_file, frames_per_iteration, early_stopping, rds):
    cap = cv2.VideoCapture(input_file)

    if not cap.isOpened():
        raise Exception(f"Could not open {input_file} with cv2.VideoCapture()")

    frames_extracted = 0
    while frames_extracted < early_stopping:
        success, frame = cap.read()
        if not success:
            print(f"Could not extract frame: #{frames_extracted}, input_file: {input_file}")
            cap.release()
            return
        else:
            rds.setArray(frames_extracted, frame)
            print(f"frame #{frames_extracted:04} was cached to redis.")
            frames_extracted += 1
        if frames_extracted % frames_per_iteration == 0:
            last_frame = frames_extracted
            first_frame = frames_extracted - frames_per_iteration
            yield (first_frame, last_frame, cap)
    cap.release()


def makeVideoInSteps(output_video_path, frames_per_iteration, rds, output_codec):
    #TODO: Install other codecs.
    output_codec = const.MP4V_CODEC

    if os.path.exists(output_video_path):
        print("Removing existing output file:", output_video_path)
        os.remove(output_video_path)
    
    frame_id = 0
    writer = None
    while 1:
        frame = rds.getArray(frame_id)
        if frame is None:
            print(
                "-------------------------------------------------------------------"
                f"\n{frame_id=} WAS NOT FOUND IN THE CACHE. RELEASEING THE WRITER.\n"
                "-------------------------------------------------------------------"
            )
            writer.release()
            return
        
        height, width = frame.shape[:2]
        if not writer:
            writer = cv2.VideoWriter(
                str(output_video_path), output_codec, const.FPS, (width, height)
            )
        print(f"frame #{frame_id:04} was written to output video.")
        writer.write(frame)
        
        frame_id += 1
        if frame_id % frames_per_iteration == 0:
            last_frame = frame_id
            first_frame = frame_id - frames_per_iteration
            print(f"Frames ({first_frame}-{last_frame-1}) written to {output_video_path}")
            yield True
