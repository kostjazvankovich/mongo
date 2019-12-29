	db.transactionViewV2.drop()
	db.runCommand(
	{
	  create: "transactionViewV2",
	  viewOn: "Transactions",
	  pipeline: [
		{
		  $lookup:
			{
			  from: "activeTransactionView",
			  let: { bookingDate: "$bookingDate", accountId: "$accountId", clientId: "$clientId" },
			  pipeline: [
				{ $match:
					{ $expr:
						{  $and:
							[
							  { $eq: [ "$clientId",  "$$clientId" ] },
							  { $eq: [ "$accountId",  "$$accountId" ] },
							]
						}
					}
				},
				{ $project: { _id: 0 } }
			  ],
			  as: "activeTransactions"
			}
		},
		{
		  $unwind: "$activeTransactions"
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
				},
			}
		},
		{
		  $project:
			{
			  _id: 0,
			  clientId: "$_id.clientId",
			  accountId: "$_id.accountId",
			  bookingDate: "$_id.bookingDate",
			  bookingDateClosingBalance: "$_id.bookingDateClosingBalance"
			}
		},
		{
		  $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1}
		},
		{
		  $group:
			{
			  _id:
				{
				  clientId: "$clientId",
				  accountId: "$accountId"
				},
			  startDate: { $first: "$bookingDate"},
			  endDate: { $last: "$bookingDate"},
			  bookingBalances: { $push: { bookingDate: "$bookingDate", bookingDateClosingBalance: "$bookingDateClosingBalance" }}
			}
		},
		{
		  $project:
			{
			  _id: 0,
			  clientId: "$_id.clientId",
			  accountId: "$_id.accountId",
			  startDate: "$startDate",
			  endDate: "$endDate",
			  bookingBalances: "$bookingBalances"
			}
		},
		{
		  $facet:
			{
			  "rangeTransactions": [
				{
				  $project:
					{
						clientId: 1,
						accountId: 1,
						endBookingDate: "$endDate"
					}
				},
				{
				  $sort: { "endBookingDate": -1 }
				},
				{
				  $addFields:
					{
					  bookingBalances:
						{
						  $map:
							{
							  input:{$range:[0, 1296000000, 86400000]},
							  in:{ 
								 bookingDate: {$subtract:["$endBookingDate", "$$this"]}, bookingDateClosingBalance: 0 }
							}
						}
					}
				},
				{
				  $group:
					{
					  _id:
						{
						  clientId: "$clientId",
						},
					  bookingBalances: { $first: "$bookingBalances"}
					}
				},
				{
					$project: 
					{
						_id: 0,
						clientId: "$_id.clientId",
						bookingBalances: "$bookingBalances",
						startBookingBalance: { $arrayElemAt: [ "$bookingBalances", -1 ] },
						endBookingBalance: { $arrayElemAt: [ "$bookingBalances", 0 ] }
					}
				},
				{
					$project: 
					{
						clientId: 1,
						category: "rangeTransactions",
						bookingBalances: 1,
						startBookingDate: "$startBookingBalance.bookingDate",
						endBookingDate: "$endBookingBalance.bookingDate"
					}
				}
			  ],
			  "lastTransactions": [
				{
				  $project:
					{
					  clientId: "$clientId",
					  accountId: "$accountId",
					  category: "lastTransactions",
					  startBookingDate: "$startDate",
					  endBookingDate: "$endDate",
					  bookingBalances: "$bookingBalances"
					}
				}
			  ]
			},
		},
		{$project: {transactions:{$setUnion:['$rangeTransactions','$lastTransactions']}}},
		{$unwind: '$transactions'},
		{$replaceRoot: { newRoot: "$transactions" }},
		{
		  $group:
			{
			  _id:
				{
				  clientId: "$clientId",
				},
			  bookingBalances: 
			  { 
				  $push: 
				  {
					startBookingDate: "$startBookingDate",
					endBookingDate: "$endBookingDate",
					category: "$category",
					bookingBalances: "$bookingBalances"
				  }
			  }
			}
		},
		{
			$project: 
			{
				"results": 
				{
					$let:{
							vars:{ 	r: {$arrayElemAt: [ {$filter:{
											input: "$bookingBalances",
											as: "bookingBalances",
											cond: { $and: [{ $eq: [ "$$bookingBalances.category", "rangeTransactions" ] }]}
										}},0]},
									t: {$filter:{
											input: "$bookingBalances",
											as: "bookingBalances",
											cond: { $and: [{ $eq: [ "$$bookingBalances.category", "lastTransactions" ] }]}
										}}
								},
							in: {$reduce: {
									input: "$$r.bookingBalances",
									initialValue: [],
									in: {$reduce: {
											input: "$$t",
											initialValue: [],
											in: {
											  $cond: [
												{
												  $and: [
													{
													  $gte:  ["$$value.bookingDate", "$$this.startBookingDate"]
													},
													{
													  $lte:  ["$$value.bookingDate", "$$this.endBookingDate"]
													}
												  ]
												},
												{
												  $arrayElemAt: [
													{
													  $filter: {
														"input": "$$this.bookingBalances",
														"as": "balance",
														"cond": {
														  "$and": [
															{"$gte": ["$$value.bookingDate", "$$balance.bookingDate"]}
														  ]
														}}
													}, -1
												  ]
												},
												{
												  $cond: [
													{
													  $gt:  ["$$value.bookingDate", "$$this.endBookingDate"]
													},
													{
													  $arrayElemAt: [ "$$this.bookingBalances", -1 ]
													},
													{
													  $arrayElemAt: [ "$$this.bookingBalances", 0 ]
													}
												  ]
												}
											  ]
											}
										}}
									}
								}
						}
				}
			}
		}
	]})
