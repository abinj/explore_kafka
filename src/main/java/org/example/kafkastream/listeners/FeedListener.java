package org.example.kafkastream.listeners;

import org.example.kafkastream.models.News;

public interface FeedListener {
    public void onNewNews(News news);
}
