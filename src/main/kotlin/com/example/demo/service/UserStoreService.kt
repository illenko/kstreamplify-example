package com.example.demo.service

import com.example.demo.api.response.UserPaymentAmount
import com.michelin.kstreamplify.service.interactivequeries.KeyValueStoreService
import org.springframework.stereotype.Service

@Service
class UserStoreService(
    private val keyValueStoreService: KeyValueStoreService,
) {
    fun getUserPaymentsAmount(userId: String): UserPaymentAmount =
        keyValueStoreService
            .getByKey("user-spending", userId)
            .let { UserPaymentAmount(userId = it.key, amount = it.value as Long) }
}
