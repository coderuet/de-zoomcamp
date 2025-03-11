# from youtube_transcript_api import YouTubeTranscriptApi


# def get_youtube_transcript(video_id: str):
#     """Fetches the transcript of a given YouTube video ID."""
#     try:
#         transcript = YouTubeTranscriptApi.get_transcript(video_id)
#         return "\n".join([entry["text"] for entry in transcript])
#     except Exception as e:
#         return f"Error: {e}"


# # Example usage
# if __name__ == "__main__":
#     video_id = "eduHi1inM4s"  # Replace with your video ID
#     transcript = get_youtube_transcript(video_id)
#     print(transcript)
