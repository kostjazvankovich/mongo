db.lastActiveTransactionView.drop()
db.runCommand({
    create: "lastActiveTransactionView",
    viewOn: "Transactions",
    pipeline: [
        {
            $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1}
        },
        {
            $group:
                {
                    _id:
                    {
                        clientId: "$clientId"
                    },
                    lastBookingDate: { $last: "$bookingDate"},
                    bookingDateClosingBalance: { $last: "$bookingDateClosingBalance"}
                }
        },
        {
            $project:
            {
                _id: 0,
                clientId: "$_id.clientId",
                lastBookingDate: "$lastBookingDate",
                bookingDateClosingBalance: "$bookingDateClosingBalance"
            }
        },
        {
            $sort:{ "clientId": 1, "accountId": 1, "lastBookingDate": 1}
        }
    ]});
