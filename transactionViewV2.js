db.transactionViewV2.drop()
db.runCommand(
  {
    create: "transactionViewV2",
    viewOn: "client",
    pipeline: [
      {
	$lookup:
	{
	  from: "Transactions",
          localField: "clientId",
          foreignField: "clientId",
	  as: "transactions"
	}
      },
      {
	$unwind: "$transactions"
      },
      {
        $project: {
          _id: 0,
          clientId: "$transactions.clientId",
          accountId: "$transactions.accountId",
          bookingDate: "$transactions.bookingDate",
          bookingDateClosingBalance: "$transactions.bookingDateClosingBalance"
        }
      },
      {
	$group:
	{
	  _id:
	  {
	    clientId: "$clientId",
            accountId: "$accountId",
	    bookingDate: "$bookingDate",
            bookingDateClosingBalance: "$bookingDateClosingBalance"
	  }
	}
      },
      {
	$group:
	{
	  _id:
	  {
	    clientId: "$_id.clientId",
            bookingDate: "$_id.bookingDate",
	  },
          bookingDateClosingBalance: { $sum: "$_id.bookingDateClosingBalance" },
          accounts: {$push: "$_id.accountId"}
	}
      },
      { 
        $sort: {"_id.clientId": 1,  "_id.bookingDate": 1}
      },
      {
        $project:
        {
          _id: 0,
          clientId: "$_id.clientId",
          bookingDate: "$_id.bookingDate",
          bookingDateClosingBalance: 1,
          accounts: 1
        }
      },
      {
        $group: 
        {
          _id: {clientId: "$clientId"},
          transactions: {$push: "$$ROOT"}
        }
      },
      {
        $project: 
        {
          _id: 0,
          clientId: "$_id.clientId",
          transactions: { $slice: ["$transactions", 14]}
        }
      }
    ]
  })
