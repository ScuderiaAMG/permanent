# Python网络视频爬虫

## 基本使用
crawler = VideoCrawler()
crawler.crawl_website("https://example.com/videos")

## 高级配置
crawler.set_download_directory("./custom_videos")
crawler.set_min_duration(600)  # 10分钟
crawler.crawl_website("https://target-site.com", max_pages=20)# permanent