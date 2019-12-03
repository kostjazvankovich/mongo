db.transactionView.drop()
db.runCommand({
    create: "transactionView",
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
                    clientId: "$clientId",
                    accountId: "$accountId",
                    bookingDate: "$bookingDate"
                },
                lastBookingDate: { $last: "$bookingDate"},
                bookingDateClosingBalance: { $last: "$bookingDateClosingBalance"}
            }
        },
        {
            $project:
            {
                clientId: "$_id.clientId",
                accountId: "$_id.accountId",
                bookingDate:
                {
                    $cond: [ {
                        $gte:
                            [
                                "$_id.bookingDate",
                                {
                                    $let:
                                        {
                                            vars: { startDate: { $subtract: ["$lastBookingDate", 1209600000]}},
                                            in: "$$startDate"
                                        }
                                }
                            ]
                    }, "$_id.bookingDate", { $subtract: ["$lastBookingDate", 1209600000]} ]
                },
                bookingDateClosingBalance: "$bookingDateClosingBalance"
            }
        },
        {
            $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1, "_id.bookingDate": 1}
        },
        {
            $group:
            {
                _id:
                {
                    clientId: "$clientId",
                    accountId: "$accountId",
                    bookingDate: "$bookingDate"
                },
                bookingDateClosingBalance:
                {
                    $last: "$bookingDateClosingBalance"
                }
            }
        },
        {
            $sort:{ "_id.clientId": 1, "_id.accountId": 1, "_id.bookingDate": 1}
        },
        {
            $group:
            {
                _id:
                {
                    clientId: "$_id.clientId",
                    bookingDate: "$_id.bookingDate"
                },
                bookingDateClosingBalance:
                {
                    $sum: "$bookingDateClosingBalance"
                }
            }
        },
        {
            $sort:{ "_id.clientId": 1, "_id.bookingDate": 1}
        },
        {
            $project:
            {
                _id: 0,
                clientId: "$_id.clientId",
                bookingDate: "$_id.bookingDate",
                bookingDateClosingBalance: "$bookingDateClosingBalance"
            }
        }
]});
