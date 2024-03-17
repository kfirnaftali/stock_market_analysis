import qrcode
img = qrcode.make("https://cloud.google.com/blog/products/data-analytics/learn-how-to-get-real-time-llm-insights-using-dataflow")
# img = qrcode.make("https://tinyurl.com/vertex-ai-vector-search")
img.save("gcp_blog_post.png")


