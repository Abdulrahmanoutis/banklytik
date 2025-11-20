from django.urls import path
from .views import (
    StatementUploadView,
    StatementListView,
    process_statement,
    statement_detail,
    review_statement,
    reprocess_statement,
    # New manual table selection views
    start_table_selection,
    select_tables,
    map_columns,
    preview_data,
    cancel_selection,
    # Chat views
    chat,
    chat_history,
)

app_name = "statements"

urlpatterns = [
    path("", StatementListView.as_view(), name="list"),
    path("upload/", StatementUploadView.as_view(), name="upload"),

    # Main processing workflow
    path("<int:pk>/process/", process_statement, name="process"),
    path("<int:pk>/reprocess/", reprocess_statement, name="reprocess"),

    # Manual table selection workflow
    path("<int:pk>/start-selection/", start_table_selection, name="start_selection"),
    path("<int:pk>/select-tables/", select_tables, name="select_tables"),
    path("<int:pk>/map-columns/", map_columns, name="map_columns"),
    path("<int:pk>/preview-data/", preview_data, name="preview_data"),
    path("<int:pk>/cancel-selection/", cancel_selection, name="cancel_selection"),

    # Viewing
    path("<int:pk>/detail/", statement_detail, name="detail"),
    path("<int:pk>/review/", review_statement, name="review"),

    # Chat interface
    path("<int:pk>/chat/", chat, name="chat"),
    path("<int:pk>/chat-history/", chat_history, name="history"),
]
