workspace "Сервис объявлений с доставкой" "Классифайд с доставкой: 10 млн DAU, 100 млн объявлений, 99,9 %" {

    model {
        buyer  = person "Покупатель" "Ищет объявления, общается в чате, оформляет заказ, оставляет отзыв"
        seller = person "Продавец" "Создаёт объявления, общается в чате, отправляет заказы"
        moder  = person "Модератор" "Проверяет объявления перед публикацией"

        payment  = softwareSystem "Payment Provider" "Внешняя платёжная система" "External"
        delivery = softwareSystem "Delivery Provider" "Внешняя служба доставки" "External"

        classifieds = softwareSystem "Сервис объявлений с доставкой" "Объявления, поиск, чат, отзывы, заказы, доставка, модерация" {

            cdn = container "CDN" "Раздаёт фото с edge, origin — S3" "CDN" "Edge"
            lb  = container "Load Balancer" "≥2 экземпляра, least connections + health-check" "L4/L7 LB" "Edge"
            gw  = container "API Gateway" "≥2 экземпляра: роутинг, rate limiting, JWT-аутентификация" "Kong / Envoy" "Edge"

            advertCreate = container "Advert Create Service" "CRUD объявлений, валидация, публикация событий" "Service"
            advertRead   = container "Advert Read Service" "Чтение объявления по id через кэш (~30 экз.)" "Service"
            resizer      = container "Image Resizer" "3 размера, ≤1 МБ" "Service"
            search       = container "Search Service" "Поиск и фильтрация, p99 < 200 мс (~12 экз.)" "Service"
            indexer      = container "Search Indexer" "Consumer Kafka, upsert в Elasticsearch" "Service"
            chat         = container "Chat Service" "Переписка покупатель ↔ продавец" "Service"
            review       = container "Review Service" "Отзывы и рейтинги" "Service"
            order        = container "Order Service" "Заказ, оплата, идемпотентность" "Service"
            delivSvc     = container "Delivery Service" "Создание и трекинг отправления" "Service"
            moderation   = container "Moderation Service" "Проверка объявлений" "Service"

            mongo   = container "MongoDB" "Source of truth: объявления, чаты, отзывы, заказы. Replica Set 1+2, шардирование hash(advert_id)" "MongoDB" "Database"
            es      = container "Elasticsearch" "Производный поисковый индекс" "Elasticsearch" "Database"
            cache   = container "Redis Cache" "Cache-aside, LFU, TTL" "Redis" "Database"
            pubsub  = container "Redis Pub/Sub" "Рассылка сообщений чата, best-effort" "Redis" "Database"
            s3      = container "S3" "Фото в трёх размерах" "Object storage" "Database"
            kafka   = container "Kafka" "События объявлений, 3 брокера, RF=3, хранение 7 дней" "Kafka" "Broker"
            paylog  = container "Payment Status Log" "Неизменяемый журнал статусов платежей, ≥5 лет" "MongoDB collection" "Database"
            observ  = container "Observability" "ELK, Prometheus, Grafana — сквозной слой" "ELK / Prometheus / Grafana"
        }

        buyer  -> classifieds "Поиск, чат, заказ, отзыв"
        seller -> classifieds "Объявления, чат, доставка"
        moder  -> classifieds "Проверка объявлений"
        classifieds -> payment "Оплата заказа"
        payment -> classifieds "Webhook: статус платежа"
        classifieds -> delivery "Создание и трекинг отправления"

        buyer  -> cdn "Фото" "HTTPS"
        buyer  -> lb "Запросы" "HTTPS"
        seller -> lb "Запросы" "HTTPS"
        moder  -> lb "Запросы" "HTTPS"
        cdn -> s3 "Origin"
        lb -> gw "Балансировка"

        gw -> advertCreate "createAdv()" "REST/JSON"
        gw -> advertRead "get(adv_id)" "REST/JSON"
        gw -> search "search()" "REST/JSON"
        gw -> chat "REST/JSON"
        gw -> review "REST/JSON"
        gw -> order "REST/JSON"

        advertCreate -> mongo "Запись объявления"
        advertCreate -> resizer "Фото"
        resizer -> s3 "3 размера"
        advertCreate -> kafka "Событие advert.created"
        advertRead -> cache "Чтение"
        cache -> mongo "Промах кэша"
        search -> es "Запрос"
        kafka -> indexer "Событие"
        indexer -> es "Upsert по advert_id"
        kafka -> moderation "Событие"
        moderation -> mongo "Статус публикации"
        chat -> pubsub "publish / subscribe"
        chat -> mongo "История"
        review -> mongo "Отзывы"
        order -> payment "initiate() + retry / Circuit Breaker" "HTTPS"
        payment -> paylog "Webhook: статус"
        paylog -> order "Обновление заказа"
        order -> delivSvc "После paid"
        delivSvc -> delivery "timeout + retry" "HTTPS"
    }

    views {
        systemContext classifieds "Context" "C4 Context" {
            include *
            autolayout lr
        }

        container classifieds "Containers" "C4 Container" {
            include *
            autolayout tb
        }

        styles {
            element "Person" {
                shape person
                background #1E2761
                color #ffffff
            }
            element "Software System" {
                background #1E2761
                color #ffffff
            }
            element "External" {
                background #999999
                color #ffffff
            }
            element "Container" {
                background #4F6BD8
                color #ffffff
            }
            element "Edge" {
                background #85B7EB
                color #000000
            }
            element "Database" {
                shape cylinder
                background #7C8DB5
                color #ffffff
            }
            element "Broker" {
                shape pipe
                background #FF7A59
                color #ffffff
            }
        }
    }
}
