# hackathon2022_02

# Задача
 Проанализировать и воссоздать приложение по предоставленным данным.
 В приложении должны быть разделы:
 1. Аналитика (в различных срезах данных)
 2. Список всех заказов с возможностью поиска по статусам и т.д.
 3. Полный жизненный цикл заказа (оформление заказа, подача предложении, исполнение заказа и т.д.)


# Описание событий:
1. OrderEvent - заказ на доставку от клиента.
2. OfferEvent - предложение от логистической компании.
3. OfferAcceptEvent - клиент принял предложение.
4. OrderCancelEvent - отмена заказа
5. OrderFreightReceiveEvent - заказ получен на отгрузку из скалада/магазина логистической компанией.
6. OrderFulfillmentEvent - заказ выполнен.
7. OrderFailEvent - заказ не выполнен.
