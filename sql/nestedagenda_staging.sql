-- All Events in the last 6 months
DROP TABLE IF EXISTS JT.Retro_Events;
CREATE TABLE JT.Retro_Events AS
SELECT ECS.*
     , Fn_Parent_BinaryVersion(BinaryVersion) AS ParentBinaryVersion
FROM EventCube.EventCubeSummary ECS
LEFT JOIN EventCube.TestEvents TE
ON ECS.ApplicationId = TE.ApplicationId
WHERE TE.ApplicationId IS NULL
AND ECS.EndDate < CURRENT_DATE
AND ECS.StartDate >= CURRENT_DATE - INTERVAL '6' Month
;

-- Flags for Events w/Features
DROP TABLE IF EXISTS JT.Retro_Events_Feature_List;
CREATE TABLE JT.Retro_Events_Feature_List AS
SELECT EVENTS.ApplicationId
     , CONFIG.Name
FROM JT.Retro_Events EVENTS
JOIN PUBLIC.Ratings_ApplicationConfigSettings CONFIG
ON EVENTS.ApplicationId = CONFIG.ApplicationId
WHERE ((CONFIG.Name = 'EnableSessionScans' AND EVENTS.ParentBinaryVersion >= '6.07' AND SettingValue ILIKE '%true%')
OR (CONFIG.Name = 'DisableStatusUpdate' AND EVENTS.ParentBinaryVersion >= '6.05' AND SettingValue ILIKE '%true%'))
UNION
SELECT EVENTS.ApplicationId
     , 'EnableNestedAgenda' AS Name
FROM JT.Retro_Events EVENTS
JOIN (SELECT DISTINCT ITEM.ApplicationId
      FROM Ratings_Item ITEM
      JOIN Ratings_Topic TOPIC
      ON ITEM.ParentTopicId = TOPIC.TopicId
      WHERE ITEM.ParentItemId IS NOT NULL
      AND ITEM.IsDisabled = 0
      AND TOPIC.IsDisabled = 0
      AND TOPIC.ListTypeId = 2
      AND TOPIC.IsHidden = false) NA
ON EVENTS.ApplicationId = NA.ApplicationId
AND EVENTS.ParentBinaryVersion >= '6.17'
;

-- Nested Agenda
-- Get all Agenda Items
DROP TABLE IF EXISTS JT.Retro_Nested_Items;
CREATE TABLE JT.Retro_Nested_Items AS
SELECT ITEM.*
FROM PUBLIC.Ratings_Item ITEM
JOIN (SELECT DISTINCT ApplicationId
      FROM JT.Retro_Events_Feature_List
      WHERE Name = 'EnableNestedAgenda'
     ) EVENT
ON ITEM.ApplicationId = EVENT.ApplicationId
JOIN PUBLIC.Ratings_Topic TOPIC
ON ITEM.ParentTopicId = TOPIC.TopicId
WHERE ITEM.IsDisabled = 0
AND TOPIC.IsDisabled = 0
AND TOPIC.IsHidden = 'false'
AND TOPIC.ListTypeId = 2
;

-- Get all Users
DROP TABLE IF EXISTS JT.Retro_Events_Nested_Users;
CREATE TABLE JT.Retro_Events_Nested_Users AS
SELECT DISTINCT USERS.ApplicationId
     , USERS.UserId
     , USERS.GlobalUserId
     , USERS.IsDisabled
FROM PUBLIC.AuthDB_IS_Users USERS
JOIN JT.Retro_Events_Feature_List EVENTS
ON USERS.ApplicationId = EVENTS.ApplicationId
WHERE EVENTS.Name = 'EnableNestedAgenda'
;

-- Session Category Breakdown 
DROP TABLE IF EXISTS JT.Retro_Nested_Items_Cat;
CREATE TABLE JT.Retro_Nested_Items_Cat AS
SELECT ITEMS.ApplicationId
     , ITEMS.ItemId
     , CASE
         WHEN ITEMS.ParentItemId IS NOT NULL THEN 'Child'
         WHEN ITEMS.ParentItemId IS NULL AND PARENTS.ParentItemId IS NULL THEN 'NonParent'
         WHEN ITEMS.ParentItemId IS NULL AND PARENTS.ParentItemId IS NOT NULL THEN 'Parent'
       END AS ItemCategory
FROM JT.Retro_Nested_Items ITEMS
LEFT JOIN (SELECT ApplicationId
                , ParentItemId
                , COUNT(*) AS ChildIdCnt
           FROM JT.Retro_Nested_Items
           WHERE ParentItemId IS NOT NULL
           GROUP BY 1,2
          ) PARENTS
ON ITEMS.ItemId = PARENTS.ParentItemId AND ITEMS.ApplicationId = PARENTS.ApplicationId
;

-- Get Relevant Nested Actions
DROP TABLE IF EXISTS JT.Retro_Nested_Actions;
CREATE TABLE JT.Retro_Nested_Actions AS
SELECT ACTIONS.*
FROM PUBLIC.Fact_Actions_Live ACTIONS
JOIN (SELECT DISTINCT LOWER(ApplicationId) AS Application_Id
      FROM JT.Retro_Events_Feature_List
      WHERE Name = 'EnableNestedAgenda'
     ) EVENT
ON ACTIONS.Application_Id = EVENT.Application_Id
JOIN JT.Retro_Events_Nested_Users USERS
ON ACTIONS.Global_User_ID = LOWER(USERS.GlobalUserId)
WHERE ACTIONS.Identifier IN ('sessionDetailButton', 'attachmentButton', 'bookmarkButton', 'itemButton')
AND USERS.IsDisabled = 0
;

-- Get Relevant Nested Views
DROP TABLE IF EXISTS JT.Retro_Nested_Views;
CREATE TABLE JT.Retro_Nested_Views AS
SELECT VIEWS.*
FROM PUBLIC.Fact_Views_Live VIEWS
JOIN (SELECT DISTINCT LOWER(ApplicationId) AS Application_Id
      FROM JT.Retro_Events_Feature_List
      WHERE Name = 'EnableNestedAgenda'
     ) EVENT
ON VIEWS.Application_Id = EVENT.Application_Id
JOIN JT.Retro_Events_Nested_Users USERS
ON VIEWS.Global_User_ID = LOWER(USERS.GlobalUserId)
WHERE VIEWS.Identifier IN ('list', 'bookmarks', 'item')
AND USERS.IsDisabled = 0
;

-- Bookmarking (iOS)
DROP TABLE IF EXISTS JT.Retro_Nested_Bookmarks_IOS_Agg;
CREATE TABLE JT.Retro_Nested_Bookmarks_IOS_Agg AS
SELECT ACTIONS.Application_Id
     , ACTIONS.Global_User_Id
     , COUNT(DISTINCT ACTIONS.Metadata->>'ItemId') AS BookmarkItemCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Parent' OR ITEMS.ItemCategory = 'NonParent' THEN ACTIONS.Metadata->>'ItemId' ELSE NULL END) AS NonChildItemBkmkCnt
     , COUNT(DISTINCT CASE WHEN (ITEMS.ItemCategory = 'Parent' OR ITEMS.ItemCategory = 'NonParent') AND Metadata->>'View' = 'list' THEN ACTIONS.Metadata->>'ItemId' ELSE NULL END) AS NonChildListViewBkmkCnt
     , COUNT(DISTINCT CASE WHEN (ITEMS.ItemCategory = 'Parent' OR ITEMS.ItemCategory = 'NonParent') AND Metadata->>'View' = 'item' THEN ACTIONS.Metadata->>'ItemId' ELSE NULL END) AS NonChildItemViewBkmkCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Child' THEN ACTIONS.Metadata->>'ItemId' ELSE NULL END) AS ChildItemBkmkCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Child' AND Metadata->>'View' = 'list' THEN ACTIONS.Metadata->>'ItemId' ELSE NULL END) AS ChildListViewBkmkCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Child' AND Metadata->>'View' = 'item' AND CAST(ACTIONS.Metadata->>'AssociatedViewItemId' AS INT) <> CAST(ACTIONS.Metadata->>'ItemId' AS INT) THEN ACTIONS.Metadata->>'ItemId' ELSE NULL END) AS ChildParentViewBkmkCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Child' AND Metadata->>'View' = 'item' AND CAST(ACTIONS.Metadata->>'AssociatedViewItemId' AS INT) = CAST(ACTIONS.Metadata->>'ItemId' AS INT) THEN ACTIONS.Metadata->>'ItemId' ELSE NULL END) AS ChildChildViewBkmkCnt
FROM JT.Retro_Nested_Actions ACTIONS
JOIN JT.Retro_Nested_Items_Cat ITEMS
ON CAST(ACTIONS.Metadata->>'ItemId' AS INT) = ITEMS.ItemId AND ACTIONS.Application_Id = LOWER(ITEMS.ApplicationId)
WHERE ACTIONS.Identifier = 'bookmarkButton'
AND (Metadata->>'View' = 'list' OR Metadata->>'View' = 'item')
AND Device_Type = 'ios'
GROUP BY 1,2
;

-- Android Item View Correction (Metrics Issue)
DROP TABLE IF EXISTS JT.Retro_Nested_Bookmarks_Android_ItemView_Corr;
CREATE TABLE JT.Retro_Nested_Bookmarks_Android_ItemView_Corr AS
SELECT SESSION_VIEW.*
FROM JT.Retro_Nested_Views SESSION_VIEW
WHERE SESSION_VIEW.Identifier = 'item'
AND SESSION_VIEW.Device_Type = 'android'
UNION ALL
SELECT SESSION_ACTIONS.*
FROM JT.Retro_Nested_Actions SESSION_ACTIONS
JOIN JT.Retro_Nested_Items_Cat ITEMS
ON CAST(SESSION_ACTIONS.Metadata->>'ItemId' AS INT) = ITEMS.ItemId AND SESSION_ACTIONS.Application_Id = LOWER(ITEMS.ApplicationId)
WHERE SESSION_ACTIONS.Identifier = 'bookmarkButton'
AND SESSION_ACTIONS.Metadata->>'View' = 'item'
AND SESSION_ACTIONS.Device_Type = 'android'
ORDER BY Application_Id, Global_User_Id, Created
;

-- Create Empty Table for Python Script Results
DROP TABLE IF EXISTS JT.Retro_Nested_Bookmarks_Android_ItemView_Corr_Final;
CREATE TABLE JT.Retro_Nested_Bookmarks_Android_ItemView_Corr_Final (
     Application_Id VARCHAR,
     Global_User_Id VARCHAR,
     Created TIMESTAMP,
     Identifier VARCHAR,
     ItemId INT,
     Metric_Type VARCHAR,
     ViewItemId INT
);


-- Bookmarking (Android)
DROP TABLE IF EXISTS JT.Retro_Nested_Bookmarks_Android_Agg;
CREATE TABLE JT.Retro_Nested_Bookmarks_Android_Agg AS
SELECT ACTIONS.Application_Id
     , ACTIONS.Global_User_Id
     , COUNT(DISTINCT ACTIONS.ItemId) AS BookmarkItemCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Parent' OR ITEMS.ItemCategory = 'NonParent' THEN ACTIONS.ItemId ELSE NULL END) AS NonChildItemBkmkCnt
     , COUNT(DISTINCT CASE WHEN (ITEMS.ItemCategory = 'Parent' OR ITEMS.ItemCategory = 'NonParent') AND ACTIONS.ViewType = 'list' THEN ACTIONS.ItemId ELSE NULL END) AS NonChildListViewBkmkCnt
     , COUNT(DISTINCT CASE WHEN (ITEMS.ItemCategory = 'Parent' OR ITEMS.ItemCategory = 'NonParent') AND ACTIONS.ViewType = 'item' THEN ACTIONS.ItemId ELSE NULL END) AS NonChildItemViewBkmkCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Child' THEN ACTIONS.ItemId ELSE NULL END) AS ChildItemBkmkCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Child' AND ACTIONS.ViewType = 'list' THEN ACTIONS.ItemId ELSE NULL END) AS ChildListViewBkmkCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Child' AND ACTIONS.ViewType = 'item' AND ACTIONS.ViewItemId <> ACTIONS.ItemId THEN ACTIONS.ItemId ELSE NULL END) AS ChildParentViewBkmkCnt
     , COUNT(DISTINCT CASE WHEN ITEMS.ItemCategory = 'Child' AND ACTIONS.ViewType = 'item' AND ACTIONS.ViewItemId = ACTIONS.ItemId THEN ACTIONS.ItemId ELSE NULL END) AS ChildChildViewBkmkCnt
FROM (SELECT *
           , 'item' AS ViewType
      FROM JT.Retro_Nested_Bookmarks_Android_ItemView_Corr_Final
      UNION ALL
      SELECT Application_Id
           , Global_User_Id
           , Created
           , Identifier
           , CAST(Metadata->>'ItemId' AS INT) AS ItemId
           , Metric_Type
           , NULL AS ViewItemId
           , 'list' AS ViewType
      FROM JT.Retro_Nested_Actions
      WHERE Identifier = 'bookmarkButton'
      AND Metadata->>'View' = 'list'
      AND Device_Type = 'android'
      ) ACTIONS
JOIN JT.Retro_Nested_Items_Cat ITEMS
ON ACTIONS.ItemId = ITEMS.ItemId AND ACTIONS.Application_Id = LOWER(ITEMS.ApplicationId)
GROUP BY 1,2
;

-- iOS and Android Results
DROP TABLE IF EXISTS JT.Retro_Nested_Bookmarks_Agg;
CREATE TABLE JT.Retro_Nested_Bookmarks_Agg AS
SELECT Application_Id
     , Global_User_Id
     , SUM(BookmarkItemCnt) AS BookmarkItemCnt
     , SUM(NonChildItemBkmkCnt) AS NonChildItemBkmkCnt
     , SUM(NonChildListViewBkmkCnt) AS NonChildListViewBkmkCnt
     , SUM(NonChildItemViewBkmkCnt) AS NonChildItemViewBkmkCnt
     , SUM(ChildItemBkmkCnt) AS ChildItemBkmkCnt
     , SUM(ChildListViewBkmkCnt) AS ChildListViewBkmkCnt
     , SUM(ChildParentViewBkmkCnt) AS ChildParentViewBkmkCnt
     , SUM(ChildChildViewBkmkCnt) AS ChildChildViewBkmkCnt
FROM (SELECT *
      FROM JT.Retro_Nested_Bookmarks_Android_Agg
      UNION ALL
      SELECT *
      FROM JT.Retro_Nested_Bookmarks_IOS_Agg
     ) A
GROUP BY 1,2
;