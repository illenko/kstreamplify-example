package com.example.demo.api.controller

import com.example.demo.service.UserStoreService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class UserController(
    private val userStoreService: UserStoreService,
) {
    @GetMapping("/users/{id}/spending")
    fun getUserPaymentsAmount(
        @PathVariable id: String,
    ) = userStoreService.getUserPaymentsAmount(id)
}
