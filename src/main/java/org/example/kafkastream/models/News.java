package org.example.kafkastream.models;

import org.bson.Document;

public class News {
    private String title;
    private String description;
    private String image;
    private String pubDate;
    private String source;
    private String category;
    private String link;

    public News(String title, String description, String image, String pubDate, String source, String category, String link) {
        this.title = title;
        this.description = description;
        this.image = image;
        this.pubDate = pubDate;
        this.source = source;
        this.category = category;
        this.link = link;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getPubDate() {
        return pubDate;
    }

    public void setPubDate(String pubDate) {
        this.pubDate = pubDate;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Document getNewsAsDocument() {
        Document newsDocument = new Document("title", title)
                .append("description", description)
                .append("image", image)
                .append("pubDate", pubDate)
                .append("source", source)
                .append("category", category)
                .append("link", link);
        return newsDocument;
    }

    public String toString() {
        return "News Object: {" +
                "title=" + title +
                "description=" + description + '\'' +
                "image=" + image + '\'' +
                "pubDate=" + pubDate + '\'' +
                "source=" + source + '\'' +
                "category=" + category + '\'' +
                "link=" + link +"}";
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }
}
